from __future__ import annotations

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"{gcs_path}")


@task()
def to_pandas(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-de-creds")

    df.to_gbq(
        destination_table="trips_data_all.rides",
        project_id="taxi-de-374816",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@task(log_prints=True)
def log_processed_rows(number_of_rows: int):
    """Write DataFrame to BiqQuery"""
    print(f"Number of processed rows: {number_of_rows}")


@flow()
def etl_gcs_to_bq(month: int, year, color: str) -> int:
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(color, year, month)
    df = to_pandas(path)
    write_bq(df)
    return df.shape[0]


@flow()
def etl_parent_flow(months: list, year, color: str):
    processed_rows = 0
    for month in months:
        processed_rows += etl_gcs_to_bq(month, year, color)
    log_processed_rows(processed_rows)


if __name__ == "__main__":
    etl_parent_flow([4], 2019, "green")
