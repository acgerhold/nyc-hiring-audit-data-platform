from src.ingestion.data_acquisition import download_files
from src.ingestion.config import Config

url_to_name = {
    Config.NYC_OPENDATA_PAYROLL_DATA_DICT: "payroll_data_dict.xlsx",
    Config.NYC_OPENDATA_PAYROLL_DATA: "payroll_data.csv",
    Config.NYC_OPENDATA_JOB_POSTINGS_DATA_DICT: "job_postings_data_dict.xlsx",
    Config.NYC_OPENDATA_JOB_POSTINGS_DATA: "job_postings_data.csv"
}

download_files(url_to_name)
