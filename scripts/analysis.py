import pandas as pd
import logging
import os
from scripts.database import get_engine
from scripts.config import LOGS_DIR, OUTPUT_DIR

# Setup Logging
logging.basicConfig(
    filename=os.path.join(LOGS_DIR, 'analysis.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_analytics(engine):
    """Runs required SQL analytics and saves to CSV."""
    logging.info("Starting analytics...")
    print("Starting analytics...")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    try:
        # a. Top 10 movies by average rating
        # We should filter for movies with a significant number of ratings to return meaningful results, 
        # but the prompt doesn't specify. I'll include a count to be safe.
        logging.info("Running: Top 10 movies by average rating")
        query_a = """
            SELECT 
                m.title, 
                AVG(f.rating) as avg_rating,
                COUNT(f.userId) as num_ratings
            FROM fact_ratings f
            JOIN dim_movies m ON f.movieId = m.movieId
            GROUP BY m.title
            HAVING num_ratings > 100 -- Arbitrary filter to avoid 1-rating 5-star movies
            ORDER BY avg_rating DESC
            LIMIT 10
        """
        df_a = pd.read_sql(query_a, engine)
        df_a.to_csv(os.path.join(OUTPUT_DIR, 'top_10_movies_by_rating.csv'), index=False)

        # b. Least 10 movies by average rating
        logging.info("Running: Least 10 movies by average rating")
        query_b = """
            SELECT 
                m.title, 
                AVG(f.rating) as avg_rating,
                COUNT(f.userId) as num_ratings
            FROM fact_ratings f
            JOIN dim_movies m ON f.movieId = m.movieId
            GROUP BY m.title
            HAVING num_ratings > 100
            ORDER BY avg_rating ASC
            LIMIT 10
        """
        df_b = pd.read_sql(query_b, engine)
        df_b.to_csv(os.path.join(OUTPUT_DIR, 'least_10_movies_by_rating.csv'), index=False)

        # c. Top 5 genres by number of ratings
        logging.info("Running: Top 5 genres by number of ratings")
        query_c = """
            SELECT 
                g.genre, 
                COUNT(f.rating) as num_ratings
            FROM fact_ratings f
            JOIN movie_genres_bridge g ON f.movieId = g.movieId
            GROUP BY g.genre
            ORDER BY num_ratings DESC
            LIMIT 5
        """
        df_c = pd.read_sql(query_c, engine)
        df_c.to_csv(os.path.join(OUTPUT_DIR, 'top_5_genres_by_ratings.csv'), index=False)

        # d. Least 5 genres by number of ratings
        logging.info("Running: Least 5 genres by number of ratings")
        query_d = """
            SELECT 
                g.genre, 
                COUNT(f.rating) as num_ratings
            FROM fact_ratings f
            JOIN movie_genres_bridge g ON f.movieId = g.movieId
            GROUP BY g.genre
            ORDER BY num_ratings ASC
            LIMIT 5
        """
        df_d = pd.read_sql(query_d, engine)
        df_d.to_csv(os.path.join(OUTPUT_DIR, 'least_5_genres_by_ratings.csv'), index=False)
        
        logging.info("Analytics completed. Results saved to output directory.")
        print("Analytics completed. Results saved to output directory.")

    except Exception as e:
        logging.error(f"Analytics failed: {e}")
        print(f"Analytics failed: {e}")
        raise

if __name__ == "__main__":
    engine = get_engine()
    run_analytics(engine)
