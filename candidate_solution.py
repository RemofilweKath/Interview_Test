# candidate_solution.py
import httpx
from pydoc import text
import sqlite3
import os
from fastapi import FastAPI, HTTPException
from typing import List, Optional
import uvicorn

# --- Constants ---
DB_NAME = "pokemon_assessment.db"


# --- Database Connection ---
def connect_db() -> Optional[sqlite3.Connection]:
    """
    Task 1: Connect to the SQLite database.
    Implement the connection logic and return the connection object.
    Return None if connection fails.
    """
    if not os.path.exists(DB_NAME):
        print(f"Error: Database file '{DB_NAME}' not found.")
        return None

    connection = None
    try:
        # --- Implement Here ---
        connection = sqlite3.connect(DB_NAME)
        connection.row_factory = sqlite3.Row
        return connection
        # --- End Implementation ---
    except sqlite3.Error as e:
        print(f"Database connection error: {e}")
        return None

    return connection


# --- Data Cleaning ---
def clean_database(conn: sqlite3.Connection):
    """
    Task 2: Clean up the database using the provided connection object.
    Implement logic to:
    - Remove duplicate entries in tables (pokemon, types, abilities, trainers).
    - Choose a consistent strategy (e.g., keep the first encountered/lowest ID).
    - Correct known misspellings (e.g., 'Pikuchu' -> 'Pikachu', 'gras' -> 'Grass', etc.).
    - Standardize casing (e.g., 'fire' -> 'Fire' or all lowercase for names/types/abilities).
    """
    if not conn:
        print("Error: Invalid database connection provided for cleaning.")
        return

    cursor = conn.cursor()
    print("Starting database cleaning...")
    
    try:
    # --- Implement Here ---
        # Known corrections for names,types,abilities, trainers
        misspellings = {
        "Pikuchu": "Pikachu",
        "gras": "Grass",
        "fir": "Fire",
        "eletric": "Electric",
        "Charmanderr": "Charmander",
        "Gary oak": "Gary Oak",
        "Ash ketchum": "Ash Ketchum",
        "Professor oak": "Professor Oak",
        "Poision": "Poison",
        }

        tables = ['pokemon', 'types', 'abilities', 'trainers']
        dirty_data = {'Remove this ability', '---', '', '???'}

        for table in tables:
            # Remove known dirty records
            for dirty_value in dirty_data:
                cursor.execute(f"DELETE FROM {table} WHERE name = ?", (dirty_value,))

            # Fix names => correct misspellings, capitalize the first letter
            cursor.execute(f"SELECT id, name FROM {table}")
            for row_id, name in cursor.fetchall():
                original_name = name
                cleaned_name = misspellings.get(name.strip(), name.strip())
                cleaned_name = cleaned_name.capitalize()

                # Only update if the name has changed
                if cleaned_name != original_name:
                    cursor.execute(f"UPDATE {table} SET name = ? WHERE id = ?", (cleaned_name, row_id))

            # Remove duplicates by keep the row with the lowest rowid for each cleaned name
            query = f"""DELETE FROM {table} WHERE rowid NOT IN (
                    SELECT MIN(rowid) FROM {table} GROUP BY TRIM(name))"""
            cursor.execute(query)
        # --- End Implementation ---
        conn.commit()
        print("Database cleaning finished and changes committed.")

    except sqlite3.Error as e:
        print(f"An error occurred during database cleaning: {e}")
        conn.rollback()  # Roll back changes on error

# --- FastAPI Application ---
def create_fastapi_app() -> FastAPI:
    """
    FastAPI application instance.
    Define the FastAPI app and include all the required endpoints below.
    """
    print("Creating FastAPI app and defining endpoints...")
    app = FastAPI(title="Pokemon Assessment API")

    # --- Define Endpoints Here ---
    @app.get("/")
    def read_root():
        """
        Task 3: Basic root response message
        Return a simple JSON response object that contains a `message` key with any corresponding value.
        """
        # --- Implement here ---
        return {"message": "Welcome to the Pokemon Assessment, The Pokemon Data API Assignment :)"}
        # --- End Implementation ---

    @app.get("/pokemon/ability/{ability_name}", response_model=List[str])
    def get_pokemon_by_ability(ability_name: str):
        """
        Task 4: Retrieve all Pokemon names with a specific ability.
        Query the cleaned database. Handle cases where the ability doesn't exist.
        """
        # --- Implement here ---
        conn = connect_db()
        cursor = conn.cursor()
        
        try:
            #Check if ability is available
            cursor.execute("SELECT id FROM abilities WHERE LOWER(name) = LOWER(?)", (ability_name.lower()))
            ability = cursor.fetchone()
            
            if not ability:
                raise HTTPException(status_code=404, detail="Ability not found")
            
            # Get a pokemon with the ability
            cursor.execute("""SELECT pokemon.name FROM pokemon JOIN pokemon_abilities ON pokemon.id = pokemon_abilities.pokemon_id
                           JOIN abilities ON pokemon_abilities.ability_id = abilities.id 
                           WHERE LOWER(abilities.name) = LOWER(?)""", (ability_name.lower(),))
            
            pokemon_list = [row['name'] for row in cursor.fetchall()]
            if not pokemon_list:
                raise HTTPException(status_code=404, detail="No Pokemon found with this ability")
            return pokemon_list
        
        except sqlite3.Error as e:
            raise HTTPException(status_code=500, detail=f"Database error: {e}")
        finally:
            conn.close()
        # --- End Implementation ---

    @app.get("/pokemon/type/{type_name}", response_model=List[str])
    def get_pokemon_by_type(type_name: str):
        """
        Task 5: Retrieve all Pokémon names of a specific type (considers type1 and type2).
        Query the cleaned database. Handle cases where the type doesn't exist.
        """
        # --- Implement here ---
        conn = connect_db()
        cursor = conn.cursor()
        
        try:
            # Check if type is available
            cursor.execute("SELECT id FROM types WHERE name = ?", (type_name,))
            type_result = cursor.fetchone()
            
            if not type_result:
                raise HTTPException(status_code=404, detail="Type not found")
            
            # Get Pokemon with the type
            # Using the LEFT JOIN is to ensure we get a Pokemon even if they have only one type
            query = """SELECT DISTINCT p.name FROM pokemon p
            LEFT JOIN types t1 ON p.type1_id = t1.id LEFT JOIN types t2 ON p.type2_id = t2.id
            WHERE t1.name = ? OR t2.name = ?;"""
            cursor.execute(query, (type_name, type_name))
            
            pokemon_list = [row['name'] for row in cursor.fetchall()]
            if not pokemon_list:
                raise HTTPException(status_code=404, detail="No Pokemon found with this type")
            return pokemon_list
        
        except sqlite3.Error as e:
            raise HTTPException(status_code=500, detail=f"Database error: {e}")
        finally:
            conn.close()
        # --- End Implementation ---

    @app.get("/trainers/pokemon/{pokemon_name}", response_model=List[str])
    def get_trainers_by_pokemon(pokemon_name: str):
        """
        Task 6: Retrieve all trainer names who have a specific Pokémon.
        Query the cleaned database. Handle cases where the Pokémon doesn't exist or has no trainer.
        """
        # --- Implement here ---
        conn = connect_db()
        cursor = conn.cursor()
        
        try:
            # Check if Pokemon exists
            cursor.execute("SELECT id FROM pokemon WHERE LOWER(name) = LOWER(?)", (pokemon_name.lower(),))
            pokemon_result = cursor.fetchone()
            
            if not pokemon_result:
                raise HTTPException(status_code=404, detail="Pokemon not found")

            # Get trainers who have the Pokemon
            cursor.execute("""SELECT trainers.name FROM trainers 
                JOIN trainer_pokemon ON trainers.id = trainer_pokemon.trainer_id 
                JOIN pokemon ON trainer_pokemon.pokemon_id = pokemon.id 
                WHERE LOWER(pokemon.name) = LOWER(?)""", (pokemon_name.lower(),))
                
            trainers_list = [row['name'] for row in cursor.fetchall()]
            if not trainers_list:
                raise HTTPException(status_code=404, detail="No trainers found with this Pokemon")
            return trainers_list
        
        except sqlite3.Error as e:
            raise HTTPException(status_code=500, detail=f"Database error: {e}")
        finally:
            conn.close()
        # --- End Implementation ---

    @app.get("/abilities/pokemon/{pokemon_name}", response_model=List[str])
    def get_abilities_by_pokemon(pokemon_name: str):
        """
        Task 7: Retrieve all ability names of a specific Pokémon.
        Query the cleaned database. Handle cases where the Pokémon doesn't exist.
        """
        # --- Implement here ---
        conn = connect_db()
        cursor = conn.cursor()
        
        try:
            # Check if Pokemon exists
            cursor.execute("SELECT id FROM pokemon WHERE LOWER(name) = LOWER(?)", (pokemon_name.lower(),))
            pokemon_result = cursor.fetchone()
            if not pokemon_result:
                raise HTTPException(status_code=404, detail="Pokemon not found")

            # Get abilities of the Pokemon
            query = """SELECT DISTINCT a.name FROM abilities a
            JOIN pokemon_abilities pa ON a.id = pa.ability_id
            JOIN pokemon p ON pa.pokemon_id = p.id WHERE LOWER(p.name) = LOWER(?);"""
            cursor.execute(query, (pokemon_name.lower(),))
                
            abilities_list = [row['name'] for row in cursor.fetchall()]
            if not abilities_list:
                raise HTTPException(status_code=404, detail="No abilities found for this Pokemon")
            return abilities_list
        
        except sqlite3.Error as e:
            raise HTTPException(status_code=500, detail=f"Database error: {e}")
        finally:
            conn.close()
        # --- End Implementation ---

    # --- Implement Task 8 here ---
    """
    Task 8: Create a new Pokemon entry from PokeAPI data.
    """
    
    def get_or_create_id(conn: sqlite3.Connection, table: str, name: str) -> int:
        """
        Checks if a record with the given name exists in the specified table (case-insensitive).
        If it exists, returns its ID. If not, inserts the new name (Title Cased) and returns the new ID.
        """
        cursor = conn.cursor()
        # Check for existing record
        cursor.execute(f"SELECT id FROM {table} WHERE LOWER(name) = LOWER(?);", (name,))
        result = cursor.fetchone()
        if not result:
            # Insert new record if not found, ensuring Title Case
            cursor.execute(f"INSERT INTO {table} (name) VALUES (?);", (name.title(),))
            conn.commit()
            # Return the last inserted ID
            return cursor.lastrowid
        return result["id"]
    
    @app.post("/pokemon/create/{pokemon_name}")
    async def create_pokemon(pokemon_name: str):
        # Fetch data from PokeAPI
        pokeapi_url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_name.lower()}"
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(pokeapi_url)
                response.raise_for_status()
                poke_data = response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise HTTPException(status_code=404, detail=f"Pokemon '{pokemon_name}' not found in PokeAPI")
            raise HTTPException(status_code=500, detail=f"PokeAPI request failed: {e}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to fetch Pokemon data: {e}")
        
        conn = connect_db()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        try:
            cursor = conn.cursor()
            
            # Get or create types
            type1_id = None
            type2_id = None
            if poke_data.get("types"):
                type1_name = poke_data["types"][0]["type"]["name"]
                type1_id = get_or_create_id(conn, "types", type1_name)
                
                if len(poke_data["types"]) > 1:
                    type2_name = poke_data["types"][1]["type"]["name"]
                    type2_id = get_or_create_id(conn, "types", type2_name)
            
            # Get or create Pokemon
            pokemon_name_proper = poke_data["name"].title()
            cursor.execute("SELECT id FROM pokemon WHERE LOWER(name) = LOWER(?)", (pokemon_name_proper,))
            pokemon_result = cursor.fetchone()
            
            if pokemon_result:
                pokemon_id = pokemon_result["id"]
                
                # Update existing Pokemon's types
                cursor.execute(
                    "UPDATE pokemon SET type1_id = ?, type2_id = ? WHERE id = ?",
                    (type1_id, type2_id, pokemon_id)
                )
            else:
                cursor.execute(
                    "INSERT INTO pokemon (name, type1_id, type2_id) VALUES (?, ?, ?)",
                    (pokemon_name_proper, type1_id, type2_id)
                )
                conn.commit()
                pokemon_id = cursor.lastrowid
            
            # Create trainer_pokemon_abilities records
            trainer_pokemon_abilities_ids = []
            abilities_data = poke_data.get("abilities", [])
            
            for ability_data in abilities_data:
                ability_name = ability_data["ability"]["name"].title()
                ability_id = get_or_create_id(conn, "abilities", ability_name)
                
                # Create pokemon_abilities link if it doesn't exist
                cursor.execute(
                    "INSERT OR IGNORE INTO pokemon_abilities (pokemon_id, ability_id) VALUES (?, ?)",
                    (pokemon_id, ability_id)
                )
                conn.commit()
                
                # Get random trainer from the table
                cursor.execute("SELECT id FROM trainers ORDER BY RANDOM() LIMIT 1")
                trainer_result = cursor.fetchone()
                
                if not trainer_result:
                    # Create default trainer if no trainers exist
                    trainer_id = get_or_create_id(conn, "trainers", "Default Trainer")
                else:
                    trainer_id = trainer_result["id"]
                
                # Create trainer_pokemon_abilities record
                cursor.execute(
                    "INSERT INTO trainer_pokemon_abilities (pokemon_id, trainer_id, ability_id) VALUES (?, ?, ?)",
                    (pokemon_id, trainer_id, ability_id)
                )
                conn.commit()
                trainer_pokemon_abilities_ids.append(cursor.lastrowid)
            
            return {
                "id": cursor.lastrowid,
            }
            
        except sqlite3.Error as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Database error: {e}")
        finally:
            conn.close()
        
    # --- End Implementation ---

    print("FastAPI app created successfully.")
    return app


# --- Main execution / Uvicorn setup (Optional - for candidate to run locally) ---
if __name__ == "__main__":
    # Ensure data is cleaned before running the app for testing
    temp_conn = connect_db()
    if temp_conn:
        clean_database(temp_conn)
        temp_conn.close()

    app_instance = create_fastapi_app()
    uvicorn.run(app_instance, host="127.0.0.1", port=8000)
