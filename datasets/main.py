import requests
import os
import io
import logging
from pathlib import Path
from typing import Dict, List
from databricks.sdk import WorkspaceClient
from datetime import datetime, timezone
from .stock_prices import (extract_company_info,
                            extract_hourly_prices,
                            write_json)
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
path = Path(__file__).parent / "output"

WS = WorkspaceClient()
BASE_PATH = "/Volumes/api_data/raw/yfinance"


def check_databricks_connection():
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    url = f"{host}/api/2.0/fs/directories{BASE_PATH}"
    try:
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=10)
        print(f"Status: {resp.status_code}")
        print(f"Response: {resp.text[:500]}")
    except requests.exceptions.RequestException as e:
        print(f"Connection failed: {e}")

def run_hourly_extraction(symbols: List[str] | None = None) -> Path:
    """Run a full hourly price extraction and persist to JSON."""
    data = extract_hourly_prices(symbols)
    return write_json(data, "hourly_prices", path)

def run_company_info_extraction(symbols: List[str] | None = None) -> Path:
    """Run a full company info extraction and persist to JSON."""
    data = extract_company_info(symbols)
    return write_json(data, "company_info", path)

def upload_files_to_databricks(local_path: Path, databricks_path: str):
    """Upload a local file to Databricks using the REST API."""

    if not local_path.exists():
        print(f"File {local_path} does not exist. Skipping upload.")
        return
    print(f"Uploading {local_path} to {databricks_path}...")
    try:
        with open(local_path, "rb") as f:
            binary_data = io.BytesIO(f.read())
            WS.files.upload(databricks_path, binary_data, overwrite=True)
        print(f"Successfully uploaded {local_path} to {databricks_path}")
    except Exception as e:
        print(f"Failed to upload {local_path}: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
    hourly_prices = f"hourly_prices_{ts}0000.json"

    print("=== Extracting hourly price data ===")
    hourly_path = run_hourly_extraction()
    print(f"Hourly data saved to: {hourly_path}")

    # print("\n=== Extracting company info ===")
    # company_path = run_company_info_extraction()
    # print(f"Company info saved to: {company_path}")

    # check_databricks_connection()
    upload_files_to_databricks(
        path / hourly_prices, f"{BASE_PATH}/hourly_prices/{hourly_prices}")
    # upload_files_to_databricks(
    #     path / "company_info.json", f"{BASE_PATH}/company_info/company_info.json")
