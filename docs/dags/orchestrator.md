# dag_orchestrator

Orchestre les trois DAGs ETL dans l'ordre, puis exécute un contrôle de qualité sur le warehouse.

<div class="dag-meta">
  <span class="dag-meta-item"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="13" height="13" fill="currentColor"><path d="M12 2C6.5 2 2 6.5 2 12s4.5 10 10 10 10-4.5 10-10S17.5 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8zm.5-13H11v6l5.25 3.15.75-1.23-4.5-2.67V7z"/></svg> <strong>Schedule</strong> 45 1 * * * — 01h45 UTC</span>
  <span class="dag-meta-item"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="13" height="13" fill="currentColor"><path d="M9 16h6v-6h4l-7-7-7 7h4zm-4 2h14v2H5z"/></svg> <strong>Cible</strong> Contrôle qualité warehouse</span>
  <span class="dag-meta-item"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="13" height="13" fill="currentColor"><path d="M15 1H9v2h6V1zm-4 13h2V8h-2v6zm8.03-6.61 1.42-1.42c-.43-.51-.9-.99-1.41-1.41l-1.42 1.42C16.07 4.74 14.12 4 12 4c-4.97 0-9 4.03-9 9s4.02 9 9 9 9-4.03 9-9c0-2.12-.74-4.07-1.97-5.61zM12 20c-3.87 0-7-3.13-7-7s3.13-7 7-7 7 3.13 7 7-3.13 7-7 7z"/></svg> <strong>Timeout</strong> 2 h</span>
  <span class="dag-meta-item"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="13" height="13" fill="currentColor"><path d="M17.65 6.35C16.2 4.9 14.21 4 12 4c-4.42 0-7.99 3.58-7.99 8s3.57 8 7.99 8c3.73 0 6.84-2.55 7.73-6h-2.08c-.82 2.33-3.04 4-5.65 4-3.31 0-6-2.69-6-6s2.69-6 6-6c1.66 0 3.14.69 4.22 1.78L13 11h7V4l-2.35 2.35z"/></svg> <strong>Retries</strong> 1 (délai 5 min)</span>
</div>

---

## Séquence d'exécution

```
trigger_ventes
      ↓
  wait_5min          ← pause de 5 min entre ventes et les deux suivants
      ↓
trigger_clients ─┐
                 ├─→  data_quality_check
trigger_produits─┘
```

`dag_clients_api` et `dag_produits_sql` sont déclenchés **en parallèle** après l'attente.
Chaque `TriggerDagRunOperator` attend la fin du DAG cible (`wait_for_completion=True`).

---

## Contrôle qualité

La tâche `data_quality_check` vérifie deux types de contraintes :

**Tables non vides**

- `fact_ventes` contient au moins une ligne
- `dim_clients` contient au moins une ligne
- `dim_produits` contient au moins une ligne

**Contraintes NOT NULL**

| Table | Colonne |
|-------|---------|
| `fact_ventes` | `date_vente` |
| `fact_ventes` | `quantite` |
| `fact_ventes` | `prix_unitaire` |
| `fact_ventes` | `canal` |
| `dim_clients` | `nom` |
| `dim_produits` | `nom` |

Si un contrôle échoue, une `ValueError` est levée — la tâche est marquée en erreur dans Airflow et le `on_failure_callback` est déclenché.

---

## Timing journalier

| Heure UTC | Événement |
|-----------|-----------|
| 01h45 | Démarrage orchestrateur |
| ~01h55 | Fin `dag_ventes_csv` |
| ~02h00 | Démarrage `dag_clients_api` + `dag_produits_sql` |
| ~02h15 | Fin des deux DAGs |
| ~02h15 | Contrôle qualité |

---

## Code

=== "Code"

    ```python title="etl/dags/dag_orchestrator.py" linenums="1"
    --8<-- "etl/dags/dag_orchestrator.py"
    ```

=== "Modules partagés"

    **`dags/callbacks.py`** — callback `on_failure` commun aux 4 DAGs

    ```python title="etl/dags/callbacks.py" linenums="1"
    --8<-- "etl/dags/callbacks.py"
    ```

    **`dags/db.py`** — context manager `get_cursor` pour PostgresHook

    ```python title="etl/dags/db.py" linenums="1"
    --8<-- "etl/dags/db.py"
    ```
