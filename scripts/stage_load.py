import pandas as pd
import os
import logging
from scripts.config import EXTRACTED_DIR, LOGS_DIR
from scripts.database import get_engine

# Setup Logging
logging.basicConfig(
    filename=os.path.join(LOGS_DIR, 'stage_load.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_movies_to_staging(engine):
    """Loads movies.csv to staging_movies table."""
    movies_path = os.path.join(EXTRACTED_DIR, 'movies.csv')
    if not os.path.exists(movies_path):
        logging.error(f"Movies file not found at {movies_path}")
        return

    logging.info("Loading movies.csv to staging_movies...")
    print("Loading movies.csv to staging_movies...")
    
    try:
        # Load entire file as it's small enough
        df = pd.read_csv(movies_path)
        df.to_sql('staging_movies', engine, if_exists='replace', index=False)
        logging.info(f"Loaded {len(df)} rows to staging_movies.")
        print(f"Loaded {len(df)} rows to staging_movies.")
    except Exception as e:
        logging.error(f"Failed to load movies: {e}")
        print(f"Failed to load movies: {e}")
        raise

def load_ratings_to_staging(engine):
    """Loads ratings.csv to staging_ratings table in chunks."""
    ratings_path = os.path.join(EXTRACTED_DIR, 'ratings.csv')
    if not os.path.exists(ratings_path):
        logging.error(f"Ratings file not found at {ratings_path}")
        return

    logging.info("Loading ratings.csv to staging_ratings...")
    print("Loading ratings.csv to staging_ratings...")

    chunk_size = 100000
    try:
        # Create table with first chunk, then append
        first_chunk = True
        for chunk in pd.read_csv(ratings_path, chunksize=chunk_size):
            if first_chunk:
                chunk.dropna(inplace=True)
                chunk.to_sql('staging_ratings', engine, if_exists='replace', index=False)
                first_chunk = False
            else:
                chunk.dropna(inplace=True)
                chunk.to_sql('staging_ratings', engine, if_exists='append', index=False)
            logging.info(f"Loaded chunk of {len(chunk)} rows to staging_ratings.")
            print(".", end="", flush=True)
        
        print("\nFinished loading ratings.")
        logging.info("Finished loading ratings.csv to staging_ratings.")
    except Exception as e:
        logging.error(f"Failed to load ratings: {e}")
        print(f"\nFailed to load ratings: {e}")
        raise

if __name__ == "__main__":
    engine = get_engine()
    try:
        load_movies_to_staging(engine)
        load_ratings_to_staging(engine)
    except Exception as e:
        print(f"Staging failed: {e}")
