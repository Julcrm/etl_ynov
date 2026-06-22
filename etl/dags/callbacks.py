"""
Callbacks Airflow partagés entre les DAGs.
"""
import logging

logger = logging.getLogger(__name__)


def on_failure(context: dict) -> None:
    """
    Callback exécuté par Airflow quand une tâche échoue.

    Loggue le DAG, la tâche, la date d'exécution et l'exception.
    """
    logger.error(
        "Echec — DAG: %s | Tâche: %s | Exécution: %s | Erreur: %s",
        context["dag"].dag_id,
        context["task_instance"].task_id,
        context["execution_date"],
        context.get("exception"),
    )
