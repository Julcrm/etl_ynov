"""
Tests unitaires pour dag_produits_sql — fonction transform_produits.
"""
import pytest
import pandas as pd
from dags.dag_produits_sql import transform_produits


def _df_produits(lignes):
    """Crée un DataFrame de test à partir d'une liste de dicts."""
    return pd.DataFrame(lignes)


def _ligne(produit_id=1, nom="Widget", categorie="A", prix=10.0,
           fournisseur="F1", actif=True, entrepot="Paris", quantite=5):
    """Construit une ligne produit × stock avec valeurs par défaut."""
    return {
        "produit_id": produit_id,
        "nom": nom,
        "categorie": categorie,
        "prix_catalogue": prix,
        "fournisseur": fournisseur,
        "actif": actif,
        "entrepot": entrepot,
        "quantite_dispo": quantite,
    }


def test_transform_produit_unique():
    """Un produit avec un seul entrepôt doit donner une ligne agrégée."""
    df = _df_produits([_ligne()])
    result = transform_produits(df)
    assert len(result) == 1


def test_transform_agregation_multi_entrepots():
    """Un produit sur plusieurs entrepôts doit donner une seule ligne agrégée."""
    df = _df_produits([
        _ligne(entrepot="Paris", quantite=5),
        _ligne(entrepot="Lyon", quantite=3),
    ])
    result = transform_produits(df)
    assert len(result) == 1


def test_transform_calcule_quantite_totale():
    """quantite_total_stock doit être la somme de tous les entrepôts."""
    df = _df_produits([
        _ligne(entrepot="Paris", quantite=5),
        _ligne(entrepot="Lyon", quantite=3),
    ])
    result = transform_produits(df)
    assert result["quantite_total_stock"].iloc[0] == 8


def test_transform_calcule_valeur_stock():
    """valeur_stock doit être SUM(quantite_dispo * prix_catalogue)."""
    df = _df_produits([
        _ligne(prix=10.0, entrepot="Paris", quantite=5),
        _ligne(prix=10.0, entrepot="Lyon", quantite=3),
    ])
    result = transform_produits(df)
    assert result["valeur_stock"].iloc[0] == pytest.approx(80.0)


def test_transform_entrepots_avec_stock_seulement():
    """entrepots_concernes ne doit lister que les entrepôts avec quantite > 0."""
    df = _df_produits([
        _ligne(entrepot="Paris", quantite=5),
        _ligne(entrepot="Lyon", quantite=0),
    ])
    result = transform_produits(df)
    entrepots = result["entrepots_concernes"].iloc[0]
    assert "Paris" in entrepots
    assert "Lyon" not in entrepots


def test_transform_entrepots_tries_alphabetiquement():
    """Les entrepôts doivent apparaître triés dans entrepots_concernes."""
    df = _df_produits([
        _ligne(entrepot="Toulouse", quantite=2),
        _ligne(entrepot="Bordeaux", quantite=4),
        _ligne(entrepot="Lyon", quantite=1),
    ])
    result = transform_produits(df)
    entrepots = result["entrepots_concernes"].iloc[0]
    assert entrepots == "Bordeaux, Lyon, Toulouse"


def test_transform_plusieurs_produits():
    """Deux produits distincts doivent donner deux lignes agrégées."""
    df = _df_produits([
        _ligne(produit_id=1, nom="Widget"),
        _ligne(produit_id=2, nom="Gadget"),
    ])
    result = transform_produits(df)
    assert len(result) == 2
    assert set(result["produit_id"].tolist()) == {1, 2}


def test_transform_stock_null_compte_comme_zero():
    """Un stock NULL (LEFT JOIN sans correspondance) ne doit pas lever d'erreur."""
    df = _df_produits([{
        "produit_id": 1,
        "nom": "Widget",
        "categorie": "A",
        "prix_catalogue": 10.0,
        "fournisseur": "F1",
        "actif": True,
        "entrepot": None,
        "quantite_dispo": None,
    }])
    result = transform_produits(df)
    assert result["quantite_total_stock"].iloc[0] == 0
    assert result["valeur_stock"].iloc[0] == pytest.approx(0.0)
