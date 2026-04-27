import json
from datetime import datetime, timezone
from typing import Dict, List
import logging
from datasets.stock_prices import (extract_company_info,
                                   extract_hourly_prices)

BASE_PATH = "/Volumes/api_data/raw/yfinance"


def run_hourly_extraction(symbols: List[str] | None = None):
    """Run a full hourly price extraction and persist to JSON."""
    data = extract_hourly_prices(symbols)
    dir_path = f"{BASE_PATH}/hourly_prices"
    return upload_to_volumes(data, dir_path, file_name="hourly_prices")


def run_company_info_extraction(symbols: List[str] | None = None):
    """Run a full company info extraction and persist to JSON."""
    data = extract_company_info(symbols)
    dir_path = f"{BASE_PATH}/company_info"
    return upload_to_volumes(data, dir_path, file_name="company_info")


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

    run_hourly_extraction()
    run_company_info_extraction()
