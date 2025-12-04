# Data-Collection-and-Preparation-SIS-2
# F1 Driver Career Data Pipeline

**Project Overview:** A robust mini-ETL pipeline designed to extract, transform, and load historical Formula 1 driver statistics. The workflow is fully automated using **Apache Airflow**, demonstrating a complete data process "from website to database."

## Key Features

* **Dynamic Scraping:** Extracts data from a JavaScript-rendered official F1 website using **Selenium**.
* **Data Quality:** Cleans, validates, and aggregates raw data (e.g., calculates total career points) using Python.
* **Normalization:** Stores career history in a **SQLite** database with a clear, relational schema. 


* **Automation:** Orchestrated via Airflow DAG, set to run once per day, including logging and retries.

---

## 1. Website Description

**Chosen Website:** Official Formula 1 Results
**URL:** `https://www.formula1.com/en/results/{year}/drivers`

The website is **dynamically rendered** (JavaScript). The scraping module (`src/scraper.py`) utilizes **Selenium** and Chromedriver to successfully access and parse the data table, specifically handling the visibility of elements and interacting with the **Cookie Consent Pop-up**.

---

## 2. Execution and Setup

### Project Structure

```text
project/
│   README.md
│   .gitignore
│   requirements.txt
│   airflow_dag.py
│   create_schema.py
│   pipeline.py
│
├── src/
│   ├── scraper.py
│   ├── cleaner.py
│   └── loader.py
│
└── data/
    ├── drivers.json
    ├── drivers_clean.json
    ├── drivers_final_cleaned.json
    └── output.db
```

## Running the Pipeline for MAC users

### 1. Environment Setup
First, create a virtual environment to isolate the project dependencies.

```bash
# 1. Create virtual environment
python3 -m venv .venv

# 2. Activate it
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
```

### 2. Configure Airflow
We need to configure Airflow to find the `airflow_dag.py` file in the main folder.

1.  **Initialize Airflow:**
    Run these commands to generate the `airflow/` folder and the configuration file:
    ```bash
    export AIRFLOW_HOME=$(pwd)/airflow
    airflow db migrate
    ```

2.  **Edit Configuration File:**
    *   Open the newly created file: `airflow/airflow.cfg`.
    *   Search for the line starting with `dags_folder`.
    *   Change it to point to your **current project folder** (where `airflow_dag.py` is located).
    
    *Example:*
    ```ini
    dags_folder = /Users/username/Desktop/Data-Collection-and-Preparation---SIS-2
    ```

### 3. Run Airflow
Now start the Airflow services:

```bash
export AIRFLOW_HOME=$(pwd)/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)
airflow standalone
```

### 4. Trigger the DAG
1.  **Access UI:** Open `http://localhost:8080` in your browser.
2.  **Login:** Use the username `admin` and the password **displayed in your terminal output**.
3.  **Run DAG:** 
    *   Locate the `f1_drivers_pipeline` DAG.
    *   Toggle the switch to **Unpause** (Blue).
    *   Click the **Play Button** (Trigger DAG) on the right.

The pipeline executes the tasks sequentially: **Scraping** → **Cleaning** → **Loading**.

---

## SQLite Database Schema

The database (`data/output.db`) contains two relational tables.

### Table 1: `drivers`
*Stores unique driver information and career totals.*

| Column Name  | Type    | Not Null | Details           |
|--------------|---------|----------|-------------------|
| id           | INTEGER | Yes      | PK, Auto Increment|
| name         | TEXT    | Yes      | UNIQUE            |
| nationality  | TEXT    | No       | -                 |
| total_points | REAL    | No       | Aggregated Sum    |
| last_updated | TEXT    | Yes      | ISO Timestamp     |

### Table 2: `career_path`
*Stores yearly results for every driver (One-to-Many relationship).*

| Column Name | Type    | Not Null | Details              |
|-------------|---------|----------|----------------------|
| id          | INTEGER | Yes      | PK, Auto Increment   |
| driver_name | TEXT    | Yes      | FK -> drivers(name)  |
| year        | INTEGER | Yes      | -                    |
| position    | TEXT    | No       | -                    |
| team        | TEXT    | No       | -                    |
| points      | REAL    | No       | -                    |

---

## Data Cleaning
Key cleaning steps performed in `src/cleaner.py`:
- **Normalization:** Removes newlines and extra spaces from names and text.
- **Type Conversion:** Converts "Year" to integer and "Points" to float.
- **Aggregation:** Calculates total career points for every driver across all years.
- **Deduplication:** Ensures no duplicate entries for the same driver in the same year.
- **Sorting:** Sorts drivers by total career points (highest to lowest).

---

## Expected Output
After running the pipeline successfully:
1.  **`data/drivers.json`** — Raw scraped data (~50 years of rows).
2.  **`data/drivers_clean.json`** — Cleaned, sorted, and aggregated data.
3.  **`data/output.db`** — SQLite database populated with relationally linked data.
