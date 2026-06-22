"""
Tests unitaires pour dag_ventes_csv — fonction clean_ventes.
"""
import pytest
import pandas as pd
from dags.dag_ventes_csv import clean_ventes


def _vente(**kwargs):
    """Construit un dict de vente minimal avec valeurs par défaut."""
    base = {
        "vente_id": 1,
        "date_vente": "2024-03-15",
        "produit_id": 10,
        "client_id": 5,
        "quantite": 2,
        "prix_unitaire": "9.99",
        "canal": "online",
    }
    base.update(kwargs)
    return base


def test_clean_supprime_doublons():
    """Les doublons sur vente_id doivent être supprimés."""
    data = [
        {"vente_id": 1, "quantite": 5},
        {"vente_id": 1, "quantite": 5},
    ]
    result = clean_ventes(data)
    assert len(result) == 1


def test_clean_supprime_quantites_negatives():
    """Les lignes avec quantite < 0 doivent être supprimées."""
    data = [
        _vente(vente_id=1, quantite=5),
        _vente(vente_id=2, quantite=-3),
        _vente(vente_id=3, quantite=0),
    ]
    result = clean_ventes(data)
    assert len(result) == 2
    assert 2 not in result["vente_id"].values


def test_clean_normalise_prix_unitaire_avec_espaces():
    """Les espaces trailing sur prix_unitaire doivent être supprimés."""
    data = [_vente(prix_unitaire=" 9.99 ")]
    result = clean_ventes(data)
    assert result["prix_unitaire"].iloc[0] == pytest.approx(9.99)


def test_clean_remplace_null_client_id():
    """Les NULL de client_id doivent être remplacés par -1."""
    data = [_vente(client_id=None)]
    result = clean_ventes(data)
    assert result["client_id"].iloc[0] == -1


def test_clean_remplace_null_produit_id():
    """Les NULL de produit_id doivent être remplacés par -1."""
    data = [_vente(produit_id=None)]
    result = clean_ventes(data)
    assert result["produit_id"].iloc[0] == -1


def test_clean_calcule_montant_total():
    """montant_total doit être égal à quantite * prix_unitaire."""
    data = [_vente(quantite=3, prix_unitaire="10.00")]
    result = clean_ventes(data)
    assert result["montant_total"].iloc[0] == pytest.approx(30.0)


def test_clean_normalise_date_iso():
    """Les dates au format YYYY-MM-DD doivent rester inchangées."""
    data = [_vente(date_vente="2024-03-15")]
    result = clean_ventes(data)
    assert result["date_vente"].iloc[0] == "2024-03-15"


def test_clean_normalise_date_francaise():
    """Les dates au format DD/MM/YYYY doivent être converties en YYYY-MM-DD."""
    data = [_vente(date_vente="15/03/2024")]
    result = clean_ventes(data)
    assert result["date_vente"].iloc[0] == "2024-03-15"


def test_clean_accepte_dataframe():
    """clean_ventes doit accepter un DataFrame en entrée."""
    df = pd.DataFrame([_vente()])
    result = clean_ventes(df)
    assert len(result) == 1


def test_clean_garde_lignes_valides():
    """Un jeu de données propre ne doit perdre aucune ligne."""
    data = [_vente(vente_id=i) for i in range(1, 6)]
    result = clean_ventes(data)
    assert len(result) == 5


def test_clean_supprime_date_invalide():
    """Une date non parseable doit entraîner la suppression de la ligne."""
    data = [
        _vente(vente_id=1, date_vente="2024-03-15"),
        _vente(vente_id=2, date_vente="pas_une_date"),
    ]
    result = clean_ventes(data)
    assert len(result) == 1
    assert result["vente_id"].iloc[0] == 1


def test_clean_supprime_ligne_sans_canal():
    """Une ligne avec canal NULL doit être supprimée (contrainte NOT NULL)."""
    data = [
        _vente(vente_id=1),
        _vente(vente_id=2, canal=None),
    ]
    result = clean_ventes(data)
    assert len(result) == 1
    assert result["vente_id"].iloc[0] == 1


def test_clean_prix_unitaire_non_numerique_supprime_ligne():
    """Un prix_unitaire non convertible en float doit supprimer la ligne."""
    data = [
        _vente(vente_id=1, prix_unitaire="9.99"),
        _vente(vente_id=2, prix_unitaire="abc"),
    ]
    result = clean_ventes(data)
    assert len(result) == 1
    assert result["vente_id"].iloc[0] == 1
