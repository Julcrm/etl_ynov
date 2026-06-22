"""
Tests unitaires pour dag_clients_api — pagination, rate limit et nettoyage.
"""
import pytest
from unittest.mock import patch, MagicMock
from dags.dag_clients_api import fetch_all_clients, _requete_avec_backoff, clean_clients


def _mock_response(status_code: int, json_data: dict, headers: dict = None):
    """Crée un mock de réponse requests."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.headers = headers or {}
    return resp


def _client(**kwargs):
    """Construit un dict client minimal avec valeurs par défaut."""
    base = {
        "id": 1,
        "nom": "Alice Dupont",
        "email": "alice@example.com",
        "telephone": "0600000000",
        "adresse": "1 rue de la Paix",
        "ville": "Paris",
        "code_postal": "75001",
        "pays": "France",
        "date_inscription": "2023-06-15",
        "statut": "actif",
    }
    base.update(kwargs)
    return base


# --- Tests fetch_all_clients ---

def test_fetch_recupere_une_seule_page():
    """Si total_pages == 1, un seul appel API doit être effectué."""
    mock_data = {
        "items": [{"id": 1, "nom": "Alice"}, {"id": 2, "nom": "Bob"}],
        "page": 1,
        "total_pages": 1,
    }
    with patch("dags.dag_clients_api.requests.get") as mock_get:
        mock_get.return_value = _mock_response(200, mock_data)
        clients = fetch_all_clients("http://api:8000")

    assert len(clients) == 2
    assert mock_get.call_count == 1


def test_fetch_pagine_correctement():
    """Toutes les pages doivent être récupérées et concaténées."""
    pages = [
        {"items": [{"id": 1, "nom": "Alice"}], "page": 1, "total_pages": 2},
        {"items": [{"id": 2, "nom": "Bob"}], "page": 2, "total_pages": 2},
    ]
    appel = 0

    def side_effect(url, params=None, timeout=None):
        nonlocal appel
        resp = _mock_response(200, pages[appel])
        appel += 1
        return resp

    with patch("dags.dag_clients_api.requests.get", side_effect=side_effect):
        with patch("dags.dag_clients_api.time.sleep"):
            clients = fetch_all_clients("http://api:8000")

    assert len(clients) == 2
    assert clients[0]["id"] == 1
    assert clients[1]["id"] == 2


def test_fetch_retourne_liste_vide_si_aucun_client():
    """L'API peut retourner zéro résultat sans erreur."""
    mock_data = {"items": [], "page": 1, "total_pages": 1}
    with patch("dags.dag_clients_api.requests.get") as mock_get:
        mock_get.return_value = _mock_response(200, mock_data)
        clients = fetch_all_clients("http://api:8000")

    assert clients == []


# --- Tests _requete_avec_backoff ---

def test_requete_gere_rate_limit_429():
    """Un code 429 doit déclencher une attente puis un retry."""
    responses = [
        _mock_response(429, {}, headers={"Retry-After": "0"}),
        _mock_response(200, {"items": [], "total_pages": 1}),
    ]
    with patch("dags.dag_clients_api.requests.get", side_effect=responses):
        with patch("dags.dag_clients_api.time.sleep"):
            result = _requete_avec_backoff("http://api:8000/clients", {})

    assert result == {"items": [], "total_pages": 1}


def test_requete_echoue_apres_max_tentatives():
    """Une série de 429 consécutifs doit lever une RuntimeError."""
    reponse_429 = _mock_response(429, {}, headers={"Retry-After": "0"})
    with patch("dags.dag_clients_api.requests.get", return_value=reponse_429):
        with patch("dags.dag_clients_api.time.sleep"):
            with pytest.raises(RuntimeError, match="Echec après"):
                _requete_avec_backoff("http://api:8000/clients", {}, max_tentatives=3)


# --- Tests clean_clients ---

def test_clean_clients_cas_nominal():
    """Un client valide doit passer sans modification."""
    result = clean_clients([_client()])
    assert len(result) == 1
    assert result[0]["id"] == 1
    assert result[0]["nom"] == "Alice Dupont"


def test_clean_clients_supprime_sans_id():
    """Un client sans id doit être ignoré."""
    result = clean_clients([_client(id=None)])
    assert len(result) == 0


def test_clean_clients_supprime_sans_nom():
    """Un client sans nom doit être ignoré."""
    result = clean_clients([_client(nom=None)])
    assert len(result) == 0


def test_clean_clients_deduplique_sur_id():
    """Deux clients avec le même id ne doivent produire qu'une entrée."""
    result = clean_clients([_client(id=1), _client(id=1, nom="Autre")])
    assert len(result) == 1


def test_clean_clients_normalise_date_inscription():
    """date_inscription doit être convertie en format YYYY-MM-DD."""
    result = clean_clients([_client(date_inscription="15/06/2023")])
    assert result[0]["date_inscription"] == "2023-06-15"


def test_clean_clients_date_invalide_devient_none():
    """Une date_inscription non parseable doit devenir None."""
    result = clean_clients([_client(date_inscription="pas_une_date")])
    assert result[0]["date_inscription"] is None


def test_clean_clients_caste_id_en_int():
    """client_id doit être un int même si l'API renvoie un float."""
    result = clean_clients([_client(id=42.0)])
    assert isinstance(result[0]["id"], int)
    assert result[0]["id"] == 42
