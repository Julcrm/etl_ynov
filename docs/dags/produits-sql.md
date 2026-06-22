# dag_produits_sql

Extrait les produits et leur stock depuis la base source via une jointure SQL, agrège les données et les charge dans `dim_produits`.

<div class="dag-meta">
  <span class="dag-meta-item"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="13" height="13" fill="currentColor"><path d="M12 2C6.5 2 2 6.5 2 12s4.5 10 10 10 10-4.5 10-10S17.5 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8zm.5-13H11v6l5.25 3.15.75-1.23-4.5-2.67V7z"/></svg> <strong>Schedule</strong> 0 3 * * * — 03h00 UTC</span>
  <span class="dag-meta-item"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="13" height="13" fill="currentColor"><path d="M19 9h-4V3H9v6H5l7 7 7-7zM5 18v2h14v-2H5z"/></svg> <strong>Source</strong> source_db · produits + stocks</span>
  <span class="dag-meta-item"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="13" height="13" fill="currentColor"><path d="M9 16h6v-6h4l-7-7-7 7h4zm-4 2h14v2H5z"/></svg> <strong>Cible</strong> warehouse_db · dim_produits</span>
  <span class="dag-meta-item"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="13" height="13" fill="currentColor"><path d="M15 1H9v2h6V1zm-4 13h2V8h-2v6zm8.03-6.61 1.42-1.42c-.43-.51-.9-.99-1.41-1.41l-1.42 1.42C16.07 4.74 14.12 4 12 4c-4.97 0-9 4.03-9 9s4.02 9 9 9 9-4.03 9-9c0-2.12-.74-4.07-1.97-5.61zM12 20c-3.87 0-7-3.13-7-7s3.13-7 7-7 7 3.13 7 7-3.13 7-7 7z"/></svg> <strong>Timeout</strong> 10 min</span>
  <span class="dag-meta-item"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="13" height="13" fill="currentColor"><path d="M17.65 6.35C16.2 4.9 14.21 4 12 4c-4.42 0-7.99 3.58-7.99 8s3.57 8 7.99 8c3.73 0 6.84-2.55 7.73-6h-2.08c-.82 2.33-3.04 4-5.65 4-3.31 0-6-2.69-6-6s2.69-6 6-6c1.66 0 3.14.69 4.22 1.78L13 11h7V4l-2.35 2.35z"/></svg> <strong>Retries</strong> 0</span>
</div>

---

## Flux

```
extract_produits  →  transform_produits  →  load_dim_produits
```

---

## Requête source

Jointure `LEFT JOIN` entre `produits` et `stocks` pour agréger par produit :

```sql
SELECT
    p.id              AS produit_id,
    p.nom,
    p.categorie,
    p.prix_catalogue,
    p.actif,
    s.entrepot,
    s.quantite_dispo
FROM produits p
LEFT JOIN stocks s ON s.produit_id = p.id
```

---

## Schéma de la table cible

| Colonne | Type | Contrainte |
|---------|------|------------|
| `produit_id` | `INTEGER` | `PRIMARY KEY` |
| `nom` | `TEXT` | `NOT NULL` |
| `categorie` | `TEXT` | — |
| `prix_catalogue` | `DECIMAL(10,2)` | — |
| `actif` | `BOOLEAN` | — |
| `quantite_total_stock` | `INTEGER` | agrégé |
| `valeur_stock` | `DECIMAL(12,2)` | agrégé |
| `entrepots_concernes` | `TEXT` | liste JSON triée |

---

## Transformations appliquées

- **Agrégation par produit** : `GROUP BY produit_id`
- `quantite_total_stock` = somme des `quantite_dispo` sur tous les entrepôts
- `valeur_stock` = `SUM(quantite_dispo × prix_catalogue)`, arrondi à 2 décimales
- `entrepots_concernes` = liste triée des entrepôts avec stock > 0, sérialisée en JSON
- Stratégie de chargement : `INSERT … ON CONFLICT (produit_id) DO UPDATE`

---

## Code & Tests

=== "Code"

    ```python title="etl/dags/dag_produits_sql.py" linenums="1"
    --8<-- "etl/dags/dag_produits_sql.py"
    ```

=== "Tests"

    ```python title="etl/tests/test_dag_produits_sql.py" linenums="1"
    --8<-- "etl/tests/test_dag_produits_sql.py"
    ```

=== "Résultats"

    ![dim_produits](../assets/dim_produits.png)
