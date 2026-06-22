# dag_clients_api

Récupère l'ensemble des clients depuis une API REST paginée, nettoie les données et les charge dans `dim_clients`.

<div class="dag-meta">
  <span class="dag-meta-item"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="13" height="13" fill="currentColor"><path d="M12 2C6.5 2 2 6.5 2 12s4.5 10 10 10 10-4.5 10-10S17.5 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8zm.5-13H11v6l5.25 3.15.75-1.23-4.5-2.67V7z"/></svg> <strong>Schedule</strong> 30 2 * * * — 02h30 UTC</span>
  <span class="dag-meta-item"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="13" height="13" fill="currentColor"><path d="M19 9h-4V3H9v6H5l7 7 7-7zM5 18v2h14v-2H5z"/></svg> <strong>Source</strong> API REST — http://api:8000/clients</span>
  <span class="dag-meta-item"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="13" height="13" fill="currentColor"><path d="M9 16h6v-6h4l-7-7-7 7h4zm-4 2h14v2H5z"/></svg> <strong>Cible</strong> warehouse_db · dim_clients</span>
  <span class="dag-meta-item"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="13" height="13" fill="currentColor"><path d="M15 1H9v2h6V1zm-4 13h2V8h-2v6zm8.03-6.61 1.42-1.42c-.43-.51-.9-.99-1.41-1.41l-1.42 1.42C16.07 4.74 14.12 4 12 4c-4.97 0-9 4.03-9 9s4.02 9 9 9 9-4.03 9-9c0-2.12-.74-4.07-1.97-5.61zM12 20c-3.87 0-7-3.13-7-7s3.13-7 7-7 7 3.13 7 7-3.13 7-7 7z"/></svg> <strong>Timeout</strong> 15 min</span>
  <span class="dag-meta-item"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="13" height="13" fill="currentColor"><path d="M17.65 6.35C16.2 4.9 14.21 4 12 4c-4.42 0-7.99 3.58-7.99 8s3.57 8 7.99 8c3.73 0 6.84-2.55 7.73-6h-2.08c-.82 2.33-3.04 4-5.65 4-3.31 0-6-2.69-6-6s2.69-6 6-6c1.66 0 3.14.69 4.22 1.78L13 11h7V4l-2.35 2.35z"/></svg> <strong>Retries</strong> 3 (délai 2 min)</span>
</div>

---

## Flux

```
fetch_clients  →  clean_clients  →  load_dim_clients
```

---

## Schéma de la table cible

| Colonne | Type | Contrainte |
|---------|------|------------|
| `client_id` | `INTEGER` | `PRIMARY KEY` |
| `nom` | `TEXT` | `NOT NULL` |
| `email` | `TEXT` | — |
| `telephone` | `TEXT` | — |
| `adresse` | `TEXT` | — |
| `ville` | `TEXT` | — |
| `code_postal` | `TEXT` | — |
| `pays` | `TEXT` | — |
| `date_inscription` | `DATE` | — |
| `statut` | `TEXT` | — |

---

## Gestion du rate limiting

L'API impose une limite de **5 requêtes/seconde**.

- Délai de **250 ms** entre chaque page (`PAGE_SIZE = 50`)
- En cas de code `HTTP 429` : attente de la valeur `Retry-After` puis retry
- Après `max_tentatives` (défaut : 5) : `RuntimeError` levée

```
Requête →  200  →  données retournées
        →  429  →  attente Retry-After  →  retry
                                        →  [×5] RuntimeError
```

---

## Transformations appliquées

- Validation `NOT NULL` sur `id` et `nom` — clients invalides rejetés
- Déduplication sur `client_id`
- Parsing de `date_inscription` — formats `YYYY-MM-DD` et `DD/MM/YYYY`
- Cast de `id` en `int` (l'API peut renvoyer des floats)
- Cast de tous les champs texte en `str`
- Stratégie de chargement : `INSERT … ON CONFLICT (client_id) DO UPDATE`

---

## Code & Tests

=== "Code"

    ```python title="etl/dags/dag_clients_api.py" linenums="1"
    --8<-- "etl/dags/dag_clients_api.py"
    ```

=== "Tests"

    ```python title="etl/tests/test_dag_clients_api.py" linenums="1"
    --8<-- "etl/tests/test_dag_clients_api.py"
    ```

=== "Résultats"

    ![dim_clients](../assets/dim_clients.png)
