"""
DAG pour l'extraction, le nettoyage et le chargement des ventes depuis un CSV.

Source  : /data/ventes.csv
Cible   : warehouse_db.fact_ventes
Schedule: tous les jours à 02:00
Timeout : 10 minutes
"""
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Union

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from dags.callbacks import on_failure
from dags.db import get_cursor

logger = logging.getLogger(__name__)

CSV_PATH = "/data/ventes.csv"
WAREHOUSE_CONN_ID = "warehouse_db"

DEFAULT_ARGS = {
    "owner": "etl",
    "retries": 0,
    "on_failure_callback": on_failure,
}


def extract_ventes(path: str = CSV_PATH) -> pd.DataFrame:
    """
    Lit le fichier CSV des ventes.

    Paramètres :
        path : chemin vers le fichier CSV

    Retourne :
        DataFrame brut
    """
    df = pd.read_csv(path)
    logger.info("Extraction : %d lignes lues depuis %s", len(df), path)
    return df


def clean_ventes(data: Union[pd.DataFrame, List[Dict[str, Any]]]) -> pd.DataFrame:
    """
    Nettoie et valide les données de ventes selon le schéma fact_ventes.

    Transformations appliquées :
    - Suppression des doublons sur vente_id
    - Normalisation de date_vente au format YYYY-MM-DD
    - Nettoyage des espaces sur prix_unitaire, conversion en float
    - Suppression des lignes avec quantite < 0
    - Remplacement des NULL de client_id / produit_id par -1
    - Calcul de montant_total = quantite * prix_unitaire
    - Validation et cast des types vers le schéma SQL cible
    - Suppression des lignes violant les contraintes NOT NULL

    Paramètres :
        data : liste de dicts ou DataFrame brut

    Retourne :
        DataFrame nettoyé et typé
    """
    df = pd.DataFrame(data) if isinstance(data, list) else data.copy()
    initial_count = len(df)

    # Déduplication par identifiant unique
    if "vente_id" in df.columns:
        df = df.drop_duplicates(subset=["vente_id"])
        logger.info("Dédoublonnage : %d doublons supprimés", initial_count - len(df))

    # Normalisation des dates (formats YYYY-MM-DD et DD/MM/YYYY)
    if "date_vente" in df.columns:
        df["date_vente"] = pd.to_datetime(
            df["date_vente"],
            format="mixed",
            dayfirst=True,
            errors="coerce",
        )
        df = df.dropna(subset=["date_vente"])
        df["date_vente"] = df["date_vente"].dt.strftime("%Y-%m-%d")

    # Suppression des espaces sur prix_unitaire
    if "prix_unitaire" in df.columns:
        df["prix_unitaire"] = pd.to_numeric(
            df["prix_unitaire"].astype(str).str.strip(), errors="coerce"
        )

    # Suppression des quantités négatives
    if "quantite" in df.columns:
        df["quantite"] = pd.to_numeric(df["quantite"], errors="coerce")
        before = len(df)
        df = df[df["quantite"] >= 0]
        logger.info("Quantités négatives : %d lignes supprimées", before - len(df))

    # Remplacement des NULL par -1 (clé étrangère inconnue)
    if "client_id" in df.columns:
        df["client_id"] = df["client_id"].fillna(-1).astype(int)
    if "produit_id" in df.columns:
        df["produit_id"] = df["produit_id"].fillna(-1).astype(int)

    # Calcul du montant total
    if "quantite" in df.columns and "prix_unitaire" in df.columns:
        df["montant_total"] = (df["quantite"] * df["prix_unitaire"]).round(2)

    # Suppression des lignes violant les contraintes NOT NULL du schéma
    # (avant le cast en str pour éviter que None ne devienne "None")
    colonnes_obligatoires = ["vente_id", "date_vente", "quantite", "prix_unitaire", "canal"]
    colonnes_presentes = [c for c in colonnes_obligatoires if c in df.columns]
    before = len(df)
    df = df.dropna(subset=colonnes_presentes)
    if len(df) < before:
        logger.warning("Contraintes NOT NULL : %d lignes invalides supprimées", before - len(df))

    # --- Validation des types vers le schéma fact_ventes ---
    casts = {
        "vente_id":     lambda s: pd.to_numeric(s, errors="coerce").astype("Int64"),
        "quantite":     lambda s: pd.to_numeric(s, errors="coerce").astype("Int64"),
        "produit_id":   lambda s: pd.to_numeric(s, errors="coerce").astype("Int64"),
        "client_id":    lambda s: pd.to_numeric(s, errors="coerce").astype("Int64"),
        "prix_unitaire": lambda s: pd.to_numeric(s, errors="coerce").round(2),
        "montant_total": lambda s: pd.to_numeric(s, errors="coerce").round(2),
        "canal":        lambda s: s.astype(str).str[:50],
    }
    for col, cast_fn in casts.items():
        if col in df.columns:
            df[col] = cast_fn(df[col])

    logger.info("Nettoyage terminé : %d lignes valides sur %d initiales", len(df), initial_count)
    return df


def load_fact_ventes(df: pd.DataFrame, conn_id: str = WAREHOUSE_CONN_ID) -> int:
    """
    Crée la table fact_ventes si besoin et insère les données (upsert).

    Stratégie : INSERT ... ON CONFLICT (vente_id) DO UPDATE.

    Paramètres :
        df      : DataFrame nettoyé
        conn_id : identifiant de connexion Airflow vers le warehouse

    Retourne :
        Nombre de lignes chargées
    """
    create_sql = """
        CREATE TABLE IF NOT EXISTS fact_ventes (
            vente_id          BIGINT PRIMARY KEY,
            date_vente        DATE NOT NULL,
            produit_id        INTEGER,
            client_id         INTEGER,
            quantite          INTEGER NOT NULL,
            prix_unitaire     DECIMAL(10, 2) NOT NULL,
            canal             VARCHAR(50) NOT NULL,
            montant_total     DECIMAL(12, 2),
            date_chargement   TIMESTAMP DEFAULT NOW(),
            date_modification TIMESTAMP DEFAULT NOW()
        )
    """

    upsert_sql = """
        INSERT INTO fact_ventes
            (vente_id, date_vente, produit_id, client_id, quantite, prix_unitaire,
             canal, montant_total, date_modification)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (vente_id) DO UPDATE SET
            date_vente        = EXCLUDED.date_vente,
            produit_id        = EXCLUDED.produit_id,
            client_id         = EXCLUDED.client_id,
            quantite          = EXCLUDED.quantite,
            prix_unitaire     = EXCLUDED.prix_unitaire,
            canal             = EXCLUDED.canal,
            montant_total     = EXCLUDED.montant_total,
            date_modification = NOW()
    """

    rows = [
        (
            int(row["vente_id"]),
            row["date_vente"],
            int(row["produit_id"]),
            int(row["client_id"]),
            int(row["quantite"]),
            float(row["prix_unitaire"]),
            str(row["canal"]),
            float(row["montant_total"]),
        )
        for _, row in df.iterrows()
    ]

    with get_cursor(conn_id) as cursor:
        cursor.execute(create_sql)
        cursor.executemany(upsert_sql, rows)

    logger.info("Chargement terminé : %d lignes dans fact_ventes", len(rows))
    return len(rows)


# --- Callables Airflow ---


def _task_extract(**context):
    """Extrait le CSV et pousse les données brutes dans XCom."""
    df = extract_ventes()
    context["ti"].xcom_push(key="ventes_raw", value=df.to_json(orient="records"))


def _task_clean(**context):
    """Récupère les données brutes de XCom, nettoie et repousse le résultat."""
    raw_json = context["ti"].xcom_pull(key="ventes_raw", task_ids="extract_csv")
    df_raw = pd.read_json(raw_json, orient="records")
    df_clean = clean_ventes(df_raw)
    context["ti"].xcom_push(key="ventes_clean", value=df_clean.to_json(orient="records"))


def _task_load(**context):
    """Récupère les données nettoyées de XCom et les charge dans le warehouse."""
    clean_json = context["ti"].xcom_pull(key="ventes_clean", task_ids="clean_data")
    df = pd.read_json(clean_json, orient="records")
    load_fact_ventes(df)


# --- Définition du DAG ---

with DAG(
    dag_id="dag_ventes_csv",
    description="ETL ventes depuis CSV vers warehouse fact_ventes",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=["etl", "csv", "ventes"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_csv",
        python_callable=_task_extract,
    )

    t_clean = PythonOperator(
        task_id="clean_data",
        python_callable=_task_clean,
    )

    t_load = PythonOperator(
        task_id="load_warehouse",
        python_callable=_task_load,
    )

    t_extract >> t_clean >> t_load
