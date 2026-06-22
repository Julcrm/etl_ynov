"""
Utilitaires de connexion base de données partagés entre les DAGs.
"""
from contextlib import contextmanager

from airflow.providers.postgres.hooks.postgres import PostgresHook


@contextmanager
def get_cursor(conn_id: str):
    """
    Context manager qui ouvre une connexion PostgreSQL via Airflow et fournit un curseur.

    Commit automatique si aucune exception, rollback sinon.
    La connexion et le curseur sont fermés dans tous les cas.

    Paramètres :
        conn_id : identifiant de connexion Airflow (ex: "warehouse_db")

    Utilisation :
        with get_cursor("warehouse_db") as cursor:
            cursor.execute("SELECT 1")
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
