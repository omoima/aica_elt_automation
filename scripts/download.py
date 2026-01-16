import os
import requests
import zipfile
import logging
from scripts.config import MOVIELENS_URL, ZIP_FILE_PATH, RAW_DATA_DIR, LOGS_DIR

# Setup Logging
logging.basicConfig(
    filename=os.path.join(LOGS_DIR, 'download.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def download_file(url, save_path):
    """Downloads a file from a URL to a specific path."""
    if os.path.exists(save_path):
        logging.info(f"File already exists at {save_path}. Skipping download.")
        print(f"File already exists at {save_path}. Skipping download.")
        return

    logging.info(f"Starting download from {url}...")
    print(f"Starting download from {url}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info(f"Download completed successfully: {save_path}")
        print(f"Download completed successfully: {save_path}")
    except Exception as e:
        logging.error(f"Failed to download file: {e}")
        print(f"Failed to download file: {e}")
        raise

def unzip_file(zip_path, extract_to):
    """Unzips a zip file to a specific directory."""
    if not os.path.exists(zip_path):
        logging.error(f"Zip file not found: {zip_path}")
        return

    logging.info(f"Extracting {zip_path} to {extract_to}...")
    print(f"Extracting {zip_path} to {extract_to}...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        logging.info("Extraction completed.")
        print("Extraction completed.")
    except Exception as e:
        logging.error(f"Failed to extract file: {e}")
        print(f"Failed to extract file: {e}")
        raise

if __name__ == "__main__":
    # Ensure directories exist
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    os.makedirs(LOGS_DIR, exist_ok=True)

    try:
        download_file(MOVIELENS_URL, ZIP_FILE_PATH)
        unzip_file(ZIP_FILE_PATH, RAW_DATA_DIR)
    except Exception as e:
        print(f"An error occurred: {e}")
