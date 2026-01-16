# aica_elt_automation
## AICA Data Engineering Track. Final Capstone Project: ELT Automation

Overview
This project contains a data pipeline that extracts data from csv files, load
to SQL database, transform to a data warehouse schema, run analytics and store the result of the
analysis as csv files. The pipeline is scheduled to run daily at 12:00PM.

## Project Structure
```
.
├── dags/                 # Apache Airflow DAGs for orchestration
│   └── movielens_etl_dag.py
├── scripts/              # Python scripts for ETL transformations
│   ├── config.py         # Configuration constants
│   ├── download.py       # Downloads and unzips data
│   ├── database.py       # Database connection and management
│   ├── stage_load.py     # Loads raw CSVs to staging tables
│   ├── transform.py      # Cleans data and creates dimension and fact tables
│   ├── validate.py       # Validates data quality
│   └── analysis.py       # Runs analytics and exports to CSV
├── data/                 # Data directory
├── logs/                 # Logs directory
├── output/               # Analytical results (CSVs)
├── requirements.txt
└── README.md
```

## Data Architecture
The pipeline uses a SQLite database (`data/movielens.db`) for storage.

1.  **Staging Layer**: Raw data from CSVs.
    *   `staging_movies`: Raw movies data.
    *   `staging_ratings`: Raw ratings data.
2.  **Transformation Layer**: Cleaned and modeled data.
    *   `dim_movies`: Dimension table for movies.
    *   `movie_genres_bridge`: Bridge table for multi-valued genres.
    *   `fact_ratings`: Fact table containing ratings and dates.

## Setup and Usage

### Prerequisites
*   Python 3.12+
*   pip

### Installation
1.  Clone the repository (or unpack the project).
2.  Create a virtual environment:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
3.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

### Running the Pipeline Manually
You can run the scripts in sequence using Python:

```bash
# 1. Download Data
python -m scripts.download

# 2. Stage and Load to DB
python -m scripts.stage_load

# 3. Validate Data
python -m scripts.validate

# 4. Transform Data
python -m scripts.transform

# 5. Run Analyics
python -m scripts.analysis
```

### Orchestration with Airflow
The DAG is defined in `dags/movielens_etl_dag.py`.
1.  Set `AIRFLOW_HOME` to the project root or configure Airflow to point to `dags/`.
2.  Start Airflow Scheduler and Webserver.
3.  Enable `movielens_etl_pipeline`.

## Deliverables
*   **Source Code**: Scripts in `scripts/`, DAG in `dags/`.
*   **Data**: Downloaded to `data/raw`.
*   **Results**: Analysis output files are generated in `output/`.
*   **Logs**: Execution logs are stored in `logs/`.

## Analytics
The following analysis output files are generated in `output/`:
*   `top_10_movies_by_rating.csv`
*   `least_10_movies_by_rating.csv`
*   `top_5_genres_by_ratings.csv`
*   `least_5_genres_by_ratings.csv`
