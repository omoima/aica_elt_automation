import os

# Base directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Data definitions
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')

# Database
DB_NAME = 'movielens.db'
DB_PATH = os.path.join(DATA_DIR, DB_NAME)
DB_CONNECTION_STRING = f'sqlite:///{DB_PATH}'

# External URLs
MOVIELENS_URL = 'https://files.grouplens.org/datasets/movielens/ml-32m.zip'
ZIP_FILE_PATH = os.path.join(RAW_DATA_DIR, 'ml-32m.zip')
EXTRACTED_DIR = os.path.join(RAW_DATA_DIR, 'ml-32m')
