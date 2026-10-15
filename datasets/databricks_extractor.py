import json
from datetime import datetime, timezone
from typing import Dict, List
import logging

try:
    from datasets.stock_prices import extract_all
except ModuleNotFoundError:
    # Supports direct execution: `python datasets/databricks_extractor.py`
    from stock_prices import extract_all

BASE_PATH = "/Volumes/api_data/raw/yfinance"


def run_extraction(symbols: List[str] | None = None):
    """Fetch yfinance once per ticker and upload hourly prices and company info."""
    hourly, company = extract_all(symbols)
    upload_to_volumes(hourly, f"{BASE_PATH}/hourly_prices", file_name="hourly_prices")
    upload_to_volumes(company, f"{BASE_PATH}/company_info", file_name="company_info")
    return hourly, company


def upload_to_volumes(data: Dict, dir_path: str, file_name: str):
    """
    Upload a dictionary to a Unity Catalog Volume.
    dir_path should look like: /Volumes/main/default/my_volume/my_folder
    """
    if file_name == "hourly_prices":
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H0000")
        file_path = f"{dir_path}/{file_name}_{ts}.json"
    else:
        file_path = f"{dir_path}/{file_name}.json"
    json_data = json.dumps(data, indent=2, default=str)
    dbutils.fs.put(file_path, json_data, overwrite=True)
    print(f"File uploaded to: {file_path}")
    return data


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    run_extraction()
