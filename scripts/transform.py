import pandas as pd
import logging
import os
from sqlalchemy import text
from scripts.database import get_engine
from scripts.config import LOGS_DIR

# Setup Logging
logging.basicConfig(
    filename=os.path.join(LOGS_DIR, 'transform.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def transform_data(engine):
    """Transforms staging data into dimensions and facts."""
    logging.info("Starting data transformation...")
    print("Starting data transformation...")

    try:
        # 1. Create dim_movies
        # Read from staging
        logging.info("Creating dim_movies...")
        movies_df = pd.read_sql("SELECT movieId, title, genres FROM staging_movies", engine)
        
        # dim_movies is just relevant columns (title), maybe extract year later
        dim_movies = movies_df[['movieId', 'title']].dropna().drop_duplicates()
        dim_movies.to_sql('dim_movies', engine, if_exists='replace', index=False)
        logging.info(f"Created dim_movies with {len(dim_movies)} rows.")

        # 2. Handle Genres (Bridge table for Analysis)
        logging.info("Creating movie_genres_bridge...")
        # Split genres by pipe |
        movies_df['genre'] = movies_df['genres'].str.split('|')
        # Explode to have one row per genre per movie
        movie_genres = movies_df.explode('genre')[['movieId', 'genre']]
        # Remove (no genres listed) if any
        movie_genres = movie_genres[movie_genres['genre'] != '(no genres listed)']
        
        movie_genres.to_sql('movie_genres_bridge', engine, if_exists='replace', index=False)
        logging.info(f"Created movie_genres_bridge with {len(movie_genres)} rows.")

        # 3. Create fact_ratings
        # For this dataset, fact_ratings is essentially staging_ratings but assured to be clean
        # We can convert timestamp to date string for readability
        logging.info("Creating fact_ratings...")
        
        # We will do this in SQL to avoid loading all 32M rows into memory just to write them back
        # SQLite supports CREATE TABLE AS SELECT
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_ratings AS 
                SELECT 
                    userId, 
                    movieId, 
                    rating, 
                    timestamp,
                    date(timestamp, 'unixepoch') as rating_date 
                FROM staging_ratings
            """))
            # If table exists (from previous run), we might want to refresh it. 
            # For simplicity in this project, we'll DROP and RECREATE
            conn.execute(text("DROP TABLE IF EXISTS fact_ratings"))
            conn.execute(text("""
                CREATE TABLE fact_ratings AS 
                SELECT 
                    userId, 
                    movieId, 
                    rating, 
                    timestamp,
                    date(timestamp, 'unixepoch') as rating_date 
                FROM staging_ratings
            """))
            conn.commit()
            
        logging.info("Created fact_ratings in database.")
        print("Data transformation completed.")

    except Exception as e:
        logging.error(f"Transformation failed: {e}")
        print(f"Transformation failed: {e}")
        raise

if __name__ == "__main__":
    engine = get_engine()
    transform_data(engine)
