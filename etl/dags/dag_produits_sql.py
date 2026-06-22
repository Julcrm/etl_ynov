"""
DAG pour l'extraction de produits et stocks depuis source_db et chargement vers warehouse.

Source  : source_db.produits JOIN source_db.stocks
Cible   : warehouse_db.dim_produits
Schedule: tous les jours à 03:00 UTC
Timeout : 10 minutes
"""
import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from dags.callbacks import on_failure
from dags.db import get_cursor

logger = logging.getLogger(__name__)

SOURCE_CONN_ID = "source_db"
WAREHOUSE_CONN_ID = "warehouse_db"

DEFAULT_ARGS = {
    "owner": "etl",
    "retries": 0,
    "on_failure_callback": on_failure,
}

REQUETE_PRODUITS_STOCKS = """
    SELECT
        p.produit_id,
        p.nom,
        p.categorie,
        p.prix_catalogue,
        p.fournisseur,
        p.actif,
        s.entrepot,
        s.quantite_dispo
    FROM produits p
    LEFT JOIN stocks s ON p.produit_id = s.produit_id
"""


def extract_produits(conn_id: str = SOURCE_CONN_ID) -> pd.DataFrame:
    """
    Extrait les produits et leurs stocks depuis source_db.

    Paramètres :
        conn_id : identifiant de connexion Airflow vers source_db

    Retourne :
        DataFrame brut avec une ligne par (produit, entrepôt)
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    df = hook.get_pandas_df(REQUETE_PRODUITS_STOCKS)
    logger.info("Extraction : %d lignes extraites (produits × stocks)", len(df))
    return df


def transform_produits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrège les données produits par produit_id.

    Calculs :
    - quantite_total_stock  = SUM(quantite_dispo) par produit
    - valeur_stock          = SUM(quantite_dispo * prix_catalogue) par produit
    - entrepots_concernes   = liste triée des entrepôts avec quantite_dispo > 0

    Paramètres :
        df : DataFrame brut issu de la jointure produits × stocks

    Retourne :
        DataFrame agrégé avec une ligne par produit
    """
    df = df.copy()
    df["quantite_dispo"] = df["quantite_dispo"].fillna(0)
    df["valeur_ligne"] = df["quantite_dispo"] * df["prix_catalogue"].fillna(0)

    agg = (
        df.groupby("produit_id")
        .agg(
            nom=("nom", "first"),
            categorie=("categorie", "first"),
            prix_catalogue=("prix_catalogue", "first"),
            fournisseur=("fournisseur", "first"),
            actif=("actif", "first"),
            quantite_total_stock=("quantite_dispo", "sum"),
            valeur_stock=("valeur_ligne", "sum"),
        )
        .reset_index()
    )

    # Entrepôts avec stock réel (quantite_dispo > 0), liste unique triée
    entrepots = (
        df[df["quantite_dispo"] > 0]
        .groupby("produit_id")["entrepot"]
        .apply(lambda x: ", ".join(sorted(x.dropna().unique())))
        .reset_index()
        .rename(columns={"entrepot": "entrepots_concernes"})
    )

    agg = agg.merge(entrepots, on="produit_id", how="left")
    agg["entrepots_concernes"] = agg["entrepots_concernes"].fillna("")
    agg["quantite_total_stock"] = agg["quantite_total_stock"].astype(int)
    agg["valeur_stock"] = agg["valeur_stock"].round(2)

    logger.info("Transformation terminée : %d produits agrégés", len(agg))
    return agg


def load_dim_produits(df: pd.DataFrame, conn_id: str = WAREHOUSE_CONN_ID) -> int:
    """
    Crée la table dim_produits si besoin et upsert les produits.

    Stratégie : INSERT ... ON CONFLICT (produit_id) DO UPDATE.

    Paramètres :
        df      : DataFrame agrégé
        conn_id : identifiant de connexion Airflow vers le warehouse

    Retourne :
        Nombre de lignes chargées
    """
    create_sql = """
        CREATE TABLE IF NOT EXISTS dim_produits (
            produit_id           INTEGER PRIMARY KEY,
            nom                  VARCHAR(255) NOT NULL,
            categorie            VARCHAR(100),
            prix_catalogue       DECIMAL(10, 2),
            fournisseur          VARCHAR(255),
            actif                BOOLEAN,
            quantite_total_stock INTEGER DEFAULT 0,
            valeur_stock         DECIMAL(15, 2) DEFAULT 0,
            entrepots_concernes  TEXT,
            date_chargement      TIMESTAMP DEFAULT NOW(),
            date_modification    TIMESTAMP DEFAULT NOW()
        )
    """

    upsert_sql = """
        INSERT INTO dim_produits
            (produit_id, nom, categorie, prix_catalogue, fournisseur, actif,
             quantite_total_stock, valeur_stock, entrepots_concernes, date_modification)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (produit_id) DO UPDATE SET
            nom                  = EXCLUDED.nom,
            categorie            = EXCLUDED.categorie,
            prix_catalogue       = EXCLUDED.prix_catalogue,
            fournisseur          = EXCLUDED.fournisseur,
            actif                = EXCLUDED.actif,
            quantite_total_stock = EXCLUDED.quantite_total_stock,
            valeur_stock         = EXCLUDED.valeur_stock,
            entrepots_concernes  = EXCLUDED.entrepots_concernes,
            date_modification    = NOW()
    """

    rows = [
        (
            int(row["produit_id"]),
            row["nom"],
            row.get("categorie"),
            float(row["prix_catalogue"]) if pd.notna(row["prix_catalogue"]) else None,
            row.get("fournisseur"),
            bool(row["actif"]) if pd.notna(row["actif"]) else None,
            int(row["quantite_total_stock"]),
            float(row["valeur_stock"]),
            row.get("entrepots_concernes", ""),
        )
        for _, row in df.iterrows()
    ]

    with get_cursor(conn_id) as cursor:
        cursor.execute(create_sql)
        cursor.executemany(upsert_sql, rows)

    logger.info("Chargement terminé : %d produits dans dim_produits", len(rows))
    return len(rows)


# --- Callables Airflow ---


def _task_extract(**context):
    """Extrait les données source et pousse dans XCom."""
    df = extract_produits()
    context["ti"].xcom_push(key="produits_raw", value=df.to_json(orient="records"))


def _task_transform(**context):
    """Récupère les données brutes de XCom, agrège et repousse."""
    raw_json = context["ti"].xcom_pull(key="produits_raw", task_ids="extract_sql")
    df_raw = pd.read_json(raw_json, orient="records")
    df_agg = transform_produits(df_raw)
    context["ti"].xcom_push(key="produits_clean", value=df_agg.to_json(orient="records"))


def _task_load(**context):
    """Récupère les données transformées de XCom et les charge dans le warehouse."""
    clean_json = context["ti"].xcom_pull(key="produits_clean", task_ids="transform_data")
    df = pd.read_json(clean_json, orient="records")
    load_dim_produits(df)


# --- Définition du DAG ---

with DAG(
    dag_id="dag_produits_sql",
    description="ETL produits depuis source_db vers warehouse dim_produits",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 3 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=["etl", "sql", "produits"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_sql",
        python_callable=_task_extract,
    )

    t_transform = PythonOperator(
        task_id="transform_data",
        python_callable=_task_transform,
    )

    t_load = PythonOperator(
        task_id="load_warehouse",
        python_callable=_task_load,
    )

    t_extract >> t_transform >> t_load
