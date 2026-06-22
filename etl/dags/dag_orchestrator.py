"""
DAG orchestrateur — déclenche les 3 DAGs ETL dans l'ordre et vérifie la qualité des données.

Séquence :
    dag_ventes_csv  →  [dag_clients_api, dag_produits_sql]  →  data_quality_check

Schedule: tous les jours à 01:45 UTC (avant les autres DAGs)
"""
import logging
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dags.db import get_cursor

logger = logging.getLogger(__name__)

WAREHOUSE_CONN_ID = "warehouse_db"

DEFAULT_ARGS = {
    "owner": "etl",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def verifier_qualite_donnees(**context):
    """
    Vérifie les contraintes de qualité sur les tables du warehouse.

    Contrôles effectués :
    - Chaque table doit contenir au moins une ligne (non vide)
    - Aucune valeur nulle sur les colonnes NOT NULL du schéma

    Lève une exception si un contrôle échoue (ce qui marque la tâche en erreur dans Airflow).
    """
    erreurs = []

    with get_cursor(WAREHOUSE_CONN_ID) as cursor:
        for table in ["fact_ventes", "dim_clients", "dim_produits"]:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            logger.info("Qualité — %s : %d lignes", table, count)
            if count == 0:
                erreurs.append(f"{table} est vide")

        controles_nulls = [
            ("fact_ventes", "date_vente"),
            ("fact_ventes", "quantite"),
            ("fact_ventes", "prix_unitaire"),
            ("fact_ventes", "canal"),
            ("dim_clients", "nom"),
            ("dim_produits", "nom"),
        ]
        for table, colonne in controles_nulls:
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {colonne} IS NULL")
            nulls = cursor.fetchone()[0]
            if nulls > 0:
                erreurs.append(f"{table}.{colonne} : {nulls} valeurs NULL interdites")

    if erreurs:
        raise ValueError("Contrôle qualité échoué :\n" + "\n".join(f"  - {e}" for e in erreurs))

    logger.info("Contrôle qualité réussi — toutes les tables sont conformes")


# --- Définition du DAG ---

with DAG(
    dag_id="dag_orchestrator",
    description="Orchestrateur ETL : déclenche les 3 DAGs et vérifie la qualité",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="45 1 * * *",
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    tags=["etl", "orchestration"],
) as dag:

    trigger_ventes = TriggerDagRunOperator(
        task_id="trigger_ventes",
        trigger_dag_id="dag_ventes_csv",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
    )

    attente = PythonOperator(
        task_id="wait_5min",
        python_callable=lambda: time.sleep(300),
    )

    trigger_clients = TriggerDagRunOperator(
        task_id="trigger_clients",
        trigger_dag_id="dag_clients_api",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
    )

    trigger_produits = TriggerDagRunOperator(
        task_id="trigger_produits",
        trigger_dag_id="dag_produits_sql",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
    )

    quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=verifier_qualite_donnees,
    )

    # dag_ventes → wait 5 min → clients + produits → qualité
    trigger_ventes >> attente >> [trigger_clients, trigger_produits] >> quality_check
