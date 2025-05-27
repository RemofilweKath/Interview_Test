import os
import sqlite3
import pytest
import tempfile
import shutil
from unittest.mock import patch, AsyncMock, MagicMock

from fastapi.testclient import TestClient
from fastapi import HTTPException

from candidate_solution import (
    connect_db,
    clean_database,
    create_fastapi_app,
    DB_NAME
)


# --- Fixtures ---

@pytest.fixture(scope="module")
def test_db(tmp_path_factory):
    # Create a temporary SQLite DB for testing
    db_path = tmp_path_factory.mktemp("data") / "pokemon_assessment.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Create minimal schema for testing
    conn.executescript("""
        CREATE TABLE pokemon (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            type1_id INTEGER,
            type2_id INTEGER
        );
        CREATE TABLE types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );
        CREATE TABLE abilities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );
        CREATE TABLE trainers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );
        CREATE TABLE trainer_pokemon_abilities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pokemon_id INTEGER,
            trainer_id INTEGER,
            ability_id INTEGER
        );
    """)

    # Insert some test data
    conn.execute("INSERT INTO types (name) VALUES ('Fire'), ('Water'), ('Grass')")
    conn.execute("INSERT INTO abilities (name) VALUES ('Blaze'), ('Torrent')")
    conn.execute("INSERT INTO trainers (name) VALUES ('Ash ketchum'), ('Misty')")
    conn.execute("INSERT INTO pokemon (name, type1_id, type2_id) VALUES ('Charmander', 1, NULL), ('Squirtle', 2, NULL)")
    conn.execute("INSERT INTO trainer_pokemon_abilities (pokemon_id, trainer_id, ability_id) VALUES (1, 1, 1), (2, 2, 2)")
    conn.commit()
    yield db_path
    conn.close()

@pytest.fixture(autouse=True)
def patch_db_name(monkeypatch, test_db):
    # Patch DB_NAME to point to our test DB
    monkeypatch.setattr("candidate_solution.DB_NAME", str(test_db))

@pytest.fixture
def client():
    app = create_fastapi_app()
    return TestClient(app)

# --- Tests ---

def test_connect_db_success():
    conn = connect_db()
    assert conn is not None
    assert isinstance(conn, sqlite3.Connection)
    conn.close()

def test_connect_db_failure(monkeypatch):
    # Patch DB_NAME to a non-existent file
    monkeypatch.setattr("candidate_solution.DB_NAME", "nonexistent.db")
    conn = connect_db()
    assert conn is None

def test_clean_database_removes_duplicates_and_misspellings():
    conn = connect_db()
    # Insert duplicates and misspellings
    conn.execute("INSERT INTO pokemon (name, type1_id, type2_id) VALUES ('Pikuchu', 1, NULL), ('Charmanderr', 1, NULL)")
    conn.execute("INSERT INTO types (name) VALUES ('gras'), ('fir')")
    conn.execute("INSERT INTO abilities (name) VALUES ('eletric'), ('---')")
    conn.execute("INSERT INTO trainers (name) VALUES ('Gary oak'), ('Ash ketchum')")
    conn.commit()

    clean_database(conn)

    # Validate results
    pokemon_names = [row["name"] for row in conn.execute("SELECT name FROM pokemon").fetchall()]
    assert not any(name.lower() == "pikuchu" for name in pokemon_names)
    assert any(name.lower() == "pikachu" for name in pokemon_names) or "Pikachu" in pokemon_names
    assert not any(name.lower() == "charmanderr" for name in pokemon_names)
    assert "Charmander" in pokemon_names

    type_names = [row["name"] for row in conn.execute("SELECT name FROM types").fetchall()]
    assert "Grass" in type_names
    assert "Fire" in type_names

    ability_names = [row["name"] for row in conn.execute("SELECT name FROM abilities").fetchall()]
    assert "Electric" in ability_names
    assert "---" not in ability_names

    trainer_names = [row["name"] for row in conn.execute("SELECT name FROM trainers").fetchall()]
    assert "Gary oak" in trainer_names
    assert "Ash ketchum" in trainer_names
    conn.close()

def test_root_endpoint(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "message" in response.json()

def test_get_pokemon_by_ability_success(client):
    """Test that ability search is case insensitive"""
    response1 = client.get("/pokemon/ability/blaze")
    response2 = client.get("/pokemon/ability/BLAZE")
    response3 = client.get("/pokemon/ability/Blaze")
    
    assert "Charmander" in response1.json()
    assert response1.status_code == 200
    assert response2.status_code == 200
    assert response3.status_code == 200
    
    # All should return the same results
    assert response1.json() == response2.json() == response3.json()

def test_get_pokemon_by_ability_not_found(client):
    response = client.get("/pokemon/ability/UnknownAbility")
    assert response.status_code == 404

def test_get_pokemon_by_type_success(client):
    response = client.get("/pokemon/type/Fire")
    assert response.status_code == 200
    assert "Charmander" in response.json()

def test_get_pokemon_by_type_not_found(client):
    response = client.get("/pokemon/type/UnknownType")
    assert response.status_code == 404

def test_get_trainers_by_pokemon_success(client):
    response = client.get("/trainers/pokemon/Charmander")
    assert response.status_code == 200
    assert "Ash Ketchum" in response.json()

def test_get_trainers_by_pokemon_not_found(client):
    response = client.get("/trainers/pokemon/Unknownmon")
    assert response.status_code == 404

def test_get_abilities_by_pokemon_success(client):
    response = client.get("/abilities/pokemon/Charmander")
    assert response.status_code == 200
    assert "Blaze" in response.json()

def test_get_abilities_by_pokemon_not_found(client):
    response = client.get("/abilities/pokemon/Unknownmon")
    assert response.status_code == 404
