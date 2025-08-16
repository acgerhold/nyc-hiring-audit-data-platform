import requests
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from tqdm import tqdm
from src.utils.logger import setup_logger
from src.ingestion.config import Config

@contextmanager
def timer():
    start = time.perf_counter()
    yield lambda: time.perf_counter() - start

def get_output_path(filename):
    data_path = Config.DATA_PATH
    match filename:
        case f if "payroll" in f:
            subfolder = f"{data_path}/payroll"
        case f if "job_postings" in f:
            subfolder = f"{data_path}/job_postings"
        case _:
            subfolder = f"{data_path}/other"
    os.makedirs(subfolder, exist_ok=True)
    return os.path.join(subfolder, filename)

# Keeping local folders as output paths for now for traceability
# May switch to temp directories later
def download_files(urls: dict):
    logger = setup_logger('ingestion', 'ingestion.log')
    events = []
    for url, filename in urls.items():
        output_path = get_output_path(filename)
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            total = int(response.headers.get('content-length', 0))
            with timer() as elapsed:
                with open(output_path, 'wb') as f, tqdm(total=total, unit='B', unit_scale=True, desc='Downloading') as pbar:
                    for chunk in response.iter_content(chunk_size=Config.CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            elapsed_time = elapsed()
            file_size = os.path.getsize(output_path)

            logger.info(f"Download complete: {output_path}")
            event = {
                "filename": filename,
                "path": output_path,
                "status": "downloaded",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            events.append(event)

            logger.info(f"File size: {file_size / (1024*1024):.2f} MB")
            logger.info(f"Elapsed time: {elapsed_time:.2f} seconds")
        except Exception as e:
            logger.error(f"Download failed: {output_path}")
            event = {
                "filename": filename,
                "path": output_path,
                "status": "failed",
                "exception": e,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            events.append(event)
            raise

    return events
