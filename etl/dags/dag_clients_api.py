"""
DAG pour l'extraction des clients depuis l'API REST et upsert vers le warehouse.

Source  : http://api:8000/clients  (paginée, rate-limited à 5 req/s)
Cible   : warehouse_db.dim_clients
Schedule: tous les jours à 02:30 UTC
Timeout : 15 minutes
"""
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from dags.callbacks import on_failure
from dags.db import get_cursor

logger = logging.getLogger(__name__)

API_BASE_URL = "http://api:8000"
WAREHOUSE_CONN_ID = "warehouse_db"
PAGE_SIZE = 50
DELAI_ENTRE_PAGES = 0.25
MAX_TENTATIVES = 5

DEFAULT_ARGS = {
    "owner": "etl",
    # Retries à 3 : l'API peut être temporairement indisponible ou en rate limit
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_failure,
}


def _requete_avec_backoff(url: str, params: Dict[str, Any], max_tentatives: int = MAX_TENTATIVES) -> dict:
    """
    Effectue une requête GET avec gestion du rate limit HTTP 429.

    En cas de 429, attend le délai indiqué par l'en-tête Retry-After
    puis réessaie. Lève une exception après max_tentatives échecs.

    Paramètres :
        url            : URL complète à appeler
        params         : paramètres de la requête
        max_tentatives : nombre max de tentatives avant abandon

    Retourne :
        Corps de la réponse JSON
    """
    for tentative in range(max_tentatives):
        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 200:
            return response.json()

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", "2"))
            logger.warning(
                "Rate limit (429) — attente de %d s (tentative %d/%d)",
                retry_after, tentative + 1, max_tentatives,
            )
            time.sleep(retry_after)
            continue

        response.raise_for_status()

    raise RuntimeError(f"Echec après {max_tentatives} tentatives sur {url}")


def fetch_all_clients(base_url: str = API_BASE_URL) -> List[Dict[str, Any]]:
    """
    Pagine l'API clients et retourne la liste complète.

    Respecte la limite de débit avec un délai entre les pages
    et gère les erreurs 429 via backoff automatique.

    Paramètres :
        base_url : URL de base de l'API

    Retourne :
        Liste de tous les clients
    """
    clients: List[Dict[str, Any]] = []
    page = 1

    while True:
        logger.info("Récupération de la page %d...", page)
        data = _requete_avec_backoff(
            f"{base_url}/clients",
            params={"page": page, "size": PAGE_SIZE},
        )

        items = data.get("items", [])
        clients.extend(items)
        total_pages = data.get("total_pages", 1)
        logger.info("Page %d/%d : %d clients (total : %d)", page, total_pages, len(items), len(clients))

        if page >= total_pages:
            break

        page += 1
        time.sleep(DELAI_ENTRE_PAGES)

    logger.info("Extraction terminée : %d clients récupérés", len(clients))
    return clients


def clean_clients(clients: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Valide et caste les données clients selon le schéma dim_clients.

    Contrôles appliqués :
    - Suppression des clients sans client_id ou sans nom (contraintes NOT NULL)
    - Déduplication sur client_id
    - Cast des types : INTEGER pour client_id, str pour les VARCHAR, DATE pour date_inscription

    Paramètres :
        clients : liste de dicts bruts issus de l'API

    Retourne :
        Liste de dicts nettoyés et typés
    """
    vus = set()
    resultat = []

    for c in clients:
        client_id = c.get("id")
        nom = c.get("nom")

        if client_id is None or nom is None:
            logger.warning("Client ignoré — id ou nom manquant : %s", c)
            continue

        if client_id in vus:
            continue
        vus.add(client_id)

        date_inscription = c.get("date_inscription")
        if date_inscription is not None:
            parsed = pd.to_datetime(date_inscription, format="mixed", dayfirst=True, errors="coerce")
            date_inscription = parsed.strftime("%Y-%m-%d") if not pd.isna(parsed) else None

        resultat.append({
            "id":               int(client_id),
            "nom":              str(nom),
            "email":            str(c["email"]) if c.get("email") is not None else None,
            "telephone":        str(c["telephone"]) if c.get("telephone") is not None else None,
            "adresse":          str(c["adresse"]) if c.get("adresse") is not None else None,
            "ville":            str(c["ville"]) if c.get("ville") is not None else None,
            "code_postal":      str(c["code_postal"]) if c.get("code_postal") is not None else None,
            "pays":             str(c["pays"]) if c.get("pays") is not None else None,
            "date_inscription": date_inscription,
            "statut":           str(c["statut"]) if c.get("statut") is not None else None,
        })

    logger.info("Nettoyage clients : %d valides sur %d reçus", len(resultat), len(clients))
    return resultat


def load_dim_clients(clients: List[Dict[str, Any]], conn_id: str = WAREHOUSE_CONN_ID) -> int:
    """
    Crée la table dim_clients si besoin et upsert les clients.

    Stratégie : INSERT ... ON CONFLICT (client_id) DO UPDATE.

    Paramètres :
        clients : liste de dicts issus de l'API
        conn_id : identifiant de connexion Airflow vers le warehouse

    Retourne :
        Nombre de lignes chargées
    """
    create_sql = """
        CREATE TABLE IF NOT EXISTS dim_clients (
            client_id         INTEGER PRIMARY KEY,
            nom               VARCHAR(255) NOT NULL,
            email             VARCHAR(255),
            telephone         VARCHAR(20),
            adresse           VARCHAR(255),
            ville             TEXT,
            code_postal       VARCHAR(10),
            pays              VARCHAR(100),
            date_inscription  DATE,
            statut            VARCHAR(20),
            date_chargement   TIMESTAMP DEFAULT NOW(),
            date_modification TIMESTAMP DEFAULT NOW()
        )
    """

    upsert_sql = """
        INSERT INTO dim_clients
            (client_id, nom, email, telephone, adresse, ville, code_postal,
             pays, date_inscription, statut, date_modification)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (client_id) DO UPDATE SET
            nom               = EXCLUDED.nom,
            email             = EXCLUDED.email,
            telephone         = EXCLUDED.telephone,
            adresse           = EXCLUDED.adresse,
            ville             = EXCLUDED.ville,
            code_postal       = EXCLUDED.code_postal,
            pays              = EXCLUDED.pays,
            date_inscription  = EXCLUDED.date_inscription,
            statut            = EXCLUDED.statut,
            date_modification = NOW()
    """

    rows = [
        (
            c["id"],
            c.get("nom"),
            c.get("email"),
            c.get("telephone"),
            c.get("adresse"),
            c.get("ville"),
            c.get("code_postal"),
            c.get("pays"),
            c.get("date_inscription"),
            c.get("statut"),
        )
        for c in clients
    ]

    with get_cursor(conn_id) as cursor:
        cursor.execute(create_sql)
        cursor.executemany(upsert_sql, rows)

    logger.info("Chargement terminé : %d clients dans dim_clients", len(rows))
    return len(rows)


# --- Callables Airflow ---


def _task_extract_clients(**context):
    """Appelle fetch_all_clients, nettoie et pousse le résultat dans XCom."""
    clients_bruts = fetch_all_clients()
    clients_clean = clean_clients(clients_bruts)
    context["ti"].xcom_push(key="clients", value=clients_clean)


def _task_load_clients(**context):
    """Récupère les clients de XCom et les charge dans le warehouse."""
    clients = context["ti"].xcom_pull(key="clients", task_ids="extract_api")
    load_dim_clients(clients)


# --- Définition du DAG ---

with DAG(
    dag_id="dag_clients_api",
    description="ETL clients depuis API REST vers warehouse dim_clients",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval="30 2 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=15),
    tags=["etl", "api", "clients"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_api",
        python_callable=_task_extract_clients,
    )

    t_load = PythonOperator(
        task_id="load_warehouse",
        python_callable=_task_load_clients,
    )

    t_extract >> t_load
