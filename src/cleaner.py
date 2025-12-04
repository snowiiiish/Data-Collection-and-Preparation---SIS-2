"""
Data Cleaning and Preprocessing F1 data from drivers into drivers_clean
"""

import json
import logging
import os
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
data_dir = os.path.join(project_root, "data")

input_file = os.path.join(data_dir, "drivers.json")
output_file = os.path.join(data_dir, "drivers_clean.json")


def load_raw_data(file_path: str) -> list:
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"Loaded {len(data)} records from {file_path}")
        return data
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return []


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace('\n', ' ')
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def convert_types(row: dict) -> dict:
    cleaned_row = row.copy()
    
    try:
        cleaned_row['Year'] = int(cleaned_row.get('Year', 0))
    except (ValueError, TypeError):
        cleaned_row['Year'] = 0

    try:
        points_str = str(cleaned_row.get('Points', '0')).strip()
        cleaned_row['Points'] = float(points_str)
    except (ValueError, TypeError):
        cleaned_row['Points'] = 0.0

    return cleaned_row


def clean_and_aggregate_data(data: list) -> list:
    logger.info(f"Starting data cleaning and aggregation for {len(data)} records")

    if not data:
        return []

    drivers_map = {}
    seen_duplicates = set()
    duplicates_count = 0
    
    for row in data:
        if not row.get('Driver'):
            continue

        driver_name = normalize_text(row['Driver'])
        nationality = normalize_text(row.get('Nationality', 'Unknown')).upper()
        team = normalize_text(row.get('Team', 'Unknown'))
        position = normalize_text(str(row.get('Position', '')))

        temp_row = {'Year': row.get('Year'), 'Points': row.get('Points')}
        converted_vals = convert_types(temp_row)
        year = converted_vals['Year']
        points = converted_vals['Points']

        unique_id = (year, driver_name)
        if unique_id in seen_duplicates:
            duplicates_count += 1
            continue
        seen_duplicates.add(unique_id)

        if driver_name not in drivers_map:
            drivers_map[driver_name] = {
                "driver_name": driver_name,
                "nationality": nationality,
                "total_points": 0.0,
                "career_history": []
            }

        drivers_map[driver_name]["total_points"] += points

        drivers_map[driver_name]["career_history"].append({
            "year": year,
            "position": position,
            "team": team,
            "points": points
        })
        
    logger.info(f"Removed {duplicates_count} duplicate records")

    final_output = list(drivers_map.values())
    
    final_output.sort(key=lambda x: x['total_points'], reverse=True)

    for driver in final_output:
        driver['career_history'].sort(key=lambda x: x['year'], reverse=True)

    return final_output


def save_cleaned_data(data: list, output_file: str):
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Cleaned data saved successfully to {output_file}")
    except IOError as e:
        logger.error(f"Error saving file: {e}")


if __name__ == "__main__":
    if not os.path.exists(data_dir):
        logger.warning(f"Data directory {data_dir} does not exist.")
        os.makedirs(data_dir, exist_ok=True)

    raw_data = load_raw_data(input_file)
    
    if raw_data:
        cleaned_data = clean_and_aggregate_data(raw_data)
        
        save_cleaned_data(cleaned_data, output_file)