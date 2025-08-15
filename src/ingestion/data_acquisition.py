import requests
import os
import time
from tqdm import tqdm
from datetime import datetime, timezone
from src.utils.logger import setup_logger

def download_files(urls: dict):
    logger = setup_logger('ingestion', 'ingestion.log')
    for url, filename in urls.items():
        start_time = time.time()
        output_path = os.path.join("data", filename)
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            total = int(response.headers.get('content-length', 0))
            chunk_size = 102400
            with open(output_path, 'wb') as f, tqdm(total=total, unit='B', unit_scale=True, desc='Downloading') as pbar:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
            elapsed = time.time() - start_time
            file_size = os.path.getsize(output_path)
            avg_speed = file_size / elapsed if elapsed > 0 else 0

            logger.info(f"Download complete: {output_path}")
            logger.info(f"File size: {file_size / (1024*1024):.2f} MB")
            logger.info(f"Elapsed time: {elapsed:.2f} seconds")
            logger.info(f"Average speed: {avg_speed / (1024*1024):.2f} MB/s")
        except Exception as e:
            logger.error(f"Download failed: {e}")
            raise
