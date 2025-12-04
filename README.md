# Data-Collection-and-Preparation---SIS-2
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
│   SIS2_Assignment.docx
│   airflow_dag.py
│   create_schema.py
│   run_pipeline.py
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

### How to Run Airflow

1.  **Start Services:** Launch the Airflow environment using Docker Compose.
    ```bash
    docker compose up -d
    ```
2.  **Access UI:** Open the Airflow web interface (`http://localhost:8080`).
3.  **Trigger DAG:** Locate the `f1_drivers_pipeline` DAG, ensure it is **ON**, and trigger a run.

The pipeline executes the tasks sequentially: **Scraping** → **Cleaning** → **Loading**.

---

## 3. Database Schema

Data is stored in `data/output.db` in a normalized schema.

| Table Name | Purpose | Key Fields | Relationship |
| :--- | :--- | :--- | :--- |
| `drivers` | Driver summary and career totals. | `id` (PK), `name` (UNIQUE) | One-to-Many |
| `career_path` | Individual seasonal results. | `id` (PK), `driver_name`, `year` | Many-to-One (`drivers`) |

---

## 4. Expected Output

Upon successful completion, the following artifacts and logs will be generated:

1.  **Database:** The **`data/output.db`** file will be created/updated.
2.  **Data Volume:** The database will contain **over 100** combined season records.
3.  **Logs:** The `loader` task logs will confirm the successful insertion of data:
