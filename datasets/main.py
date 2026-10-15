import requests
import os
import io
import logging
from pathlib import Path
from typing import Dict, List, Tuple
from databricks.sdk import WorkspaceClient
from .stock_prices import extract_all, write_json
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

def run_extraction(symbols: List[str] | None = None) -> Tuple[Path, Path]:
    """Fetch yfinance once per ticker and persist hourly prices and company info to JSON."""
    hourly, company = extract_all(symbols)
    return (
        write_json(hourly, "hourly_prices", path),
        write_json(company, "company_info", path, timestamped=False),
    )

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

    print("=== Extracting hourly prices and company info ===")
    hourly_path, company_path = run_extraction()
    print(f"Hourly data saved to: {hourly_path}")
    print(f"Company info saved to: {company_path}")

    # check_databricks_connection()
    upload_files_to_databricks(
        hourly_path,
        f"{BASE_PATH}/hourly_prices/{hourly_path.name}"
)
    upload_files_to_databricks(
        company_path, 
        f"{BASE_PATH}/company_info/company_info.json")
