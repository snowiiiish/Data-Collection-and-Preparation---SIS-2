"""
Loader loads cleaned F1 data from drivers_clean into SQLite database.
"""

import json
import logging
import os
import sqlite3
from datetime import datetime


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
data_dir = os.path.join(project_root, "data")

input_file = os.path.join(data_dir, "drivers_clean.json")
db_file = os.path.join(data_dir, "output.db")


def create_database_schema(db_path: str = "data/output.db"):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS drivers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        nationality TEXT,
        total_points REAL,
        last_updated TEXT
    );
    """)
 
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS career_path (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        driver_name TEXT NOT NULL,
        year INTEGER,
        position TEXT,
        team TEXT,
        points REAL,
        last_updated TEXT,
        FOREIGN KEY (driver_name) REFERENCES drivers (name),
        UNIQUE(driver_name, year)
    );
    """)
    
    conn.commit()
    conn.close()
    
    logger.info(f"Database at {db_path}")


def load_cleaned_data(input_file: str) -> list:
    if not os.path.exists(input_file):
        logger.error(f"File not found: {input_file}")
        return []

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"Loaded {len(data)} records from {input_file}")
        return data
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return []


def insert_data_to_db(data: list, db_path: str = "data/output.db"):
    if not data:
        logger.warning("No data to insert")
        return
    
    create_database_schema(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    updated_at = datetime.now().isoformat()
   
    counter = {
        "drivers_inserted": 0,
        "drivers_updated": 0,
        "career_inserted": 0,
        "career_updated": 0,
        "deleted_old": 0,
        "errors": 0
    }
    
    try:
        for entry in data:
            driver_name = entry['driver_name']

            try:
                cursor.execute("SELECT id FROM drivers WHERE name = ?", (driver_name,))
                exists = cursor.fetchone()

                if exists:
                    cursor.execute("""
                        UPDATE drivers 
                        SET nationality = ?, total_points = ?, last_updated = ?
                        WHERE name = ?
                    """, (entry['nationality'], entry['total_points'], updated_at, driver_name))
                    counter["drivers_updated"] += 1
                else:
                    cursor.execute("""
                        INSERT INTO drivers (name, nationality, total_points, last_updated)
                        VALUES (?, ?, ?, ?)
                    """, (driver_name, entry['nationality'], entry['total_points'], updated_at))
                    counter["drivers_inserted"] += 1

                for season in entry['career_history']:
                    year = season['year']
                    
                    cursor.execute("SELECT id FROM career_path WHERE driver_name = ? AND year = ?", (driver_name, year))
                    career_exists = cursor.fetchone()

                    if career_exists:
                        cursor.execute("""
                            UPDATE career_path 
                            SET position = ?, team = ?, points = ?, last_updated = ?
                            WHERE driver_name = ? AND year = ?
                        """, (season['position'], season['team'], season['points'], updated_at, driver_name, year))
                        counter["career_updated"] += 1
                    else:
                        cursor.execute("""
                            INSERT INTO career_path(driver_name, year, position, team, points, last_updated)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (driver_name, year, season['position'], season['team'], season['points'], updated_at))
                        counter["career_inserted"] += 1

            except sqlite3.Error as e:
                logger.error(f"SQL Error processing {driver_name}: {e}")
                counter["errors"] += 1

        cursor.execute("DELETE FROM career_path WHERE last_updated < ?", (updated_at,))
        deleted_career = cursor.rowcount
        
        cursor.execute("DELETE FROM drivers WHERE last_updated < ?", (updated_at,))
        deleted_drivers = cursor.rowcount
        
        counter["deleted_old"] = deleted_career + deleted_drivers


        conn.commit()
        
        logger.info(f"Drivers in db: {counter['drivers_inserted']} New | {counter['drivers_updated']} Updated")
        logger.info(f"Career history : {counter['career_inserted']} New | {counter['career_updated']} Updated")
        logger.info(f"Deleted: {counter['deleted_old']} old records")
        if counter['errors'] > 0:
            logger.warning(f"Errors : {counter['errors']}")

    except Exception as e:
        logger.error(f"Critical Database Error: {e}")
        conn.rollback()
    finally:
        conn.close()


def verify_database(db_path: str = "data/output.db") -> dict:
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM drivers")
        total_drivers = cursor.fetchone()[0]
        
        
        conn.close()
        logger.info(f"Verification: DB contains {total_drivers} drivers")
        
    except Exception as e:
        logger.error(f"Error verifying database: {e}")
        return {}


def run_loader():
    logger.info("Starting Loader")
    cleaned_data = load_cleaned_data(input_file)
    insert_data_to_db(cleaned_data, db_file)
    verify_database(db_file)
    logger.info("Loader Finished")


if __name__ == "__main__":
    run_loader()