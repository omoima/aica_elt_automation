import pandas as pd
import logging
import os
from scripts.database import get_engine
from scripts.config import LOGS_DIR

# Setup Logging
logging.basicConfig(
    filename=os.path.join(LOGS_DIR, 'validate.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_validations(engine):
    """Runs data quality checks on staging tables."""
    logging.info("Starting data validation...")
    print("Starting data validation...")

    valid = True

    # 1. Check for Nulls in Movies
    try:
        null_movies = pd.read_sql("SELECT COUNT(*) as count FROM staging_movies WHERE movieId IS NULL OR title IS NULL", engine)
        if null_movies['count'][0] > 0:
            logging.warning(f"Found {null_movies['count'][0]} movies with null IDs or titles.")
            valid = False
        else:
            logging.info("No null movies found.")
    except Exception as e:
        logging.error(f"Validation failed for movies: {e}")
        valid = False

    # 2. Check for Nulls in Ratings
    try:
        null_ratings = pd.read_sql("SELECT COUNT(*) as count FROM staging_ratings WHERE userId IS NULL OR movieId IS NULL OR rating IS NULL", engine)
        if null_ratings['count'][0] > 0:
            logging.warning(f"Found {null_ratings['count'][0]} ratings with null IDs or scores.")
            valid = False
        else:
            logging.info("No null ratings found.")
    except Exception as e:
        logging.error(f"Validation failed for ratings: {e}")
        valid = False

    # 3. Validate Rating Range
    try:
        invalid_ratings = pd.read_sql("SELECT COUNT(*) as count FROM staging_ratings WHERE rating < 0 OR rating > 5", engine)
        if invalid_ratings['count'][0] > 0:
            logging.warning(f"Found {invalid_ratings['count'][0]} ratings out of range (0-5).")
            valid = False
        else:
            logging.info("All ratings are within valid range (0-5).")
    except Exception as e:
        logging.error(f"Validation failed for rating range: {e}")
        valid = False

    if valid:
        logging.info("Data validation passed.")
        print("Data validation passed.")
    else:
        logging.warning("Data validation finished with issues. Check logs.")
        print("Data validation finished with issues.")

if __name__ == "__main__":
    engine = get_engine()
    run_validations(engine)
