from prefect.deployments import Deployment
from prefect.filesystems import GitHub

from etl_web_to_gcs import etl_web_to_gcs


github_block = GitHub.load("github-week-02")

github_deploy = Deployment.build_from_flow(
     flow=etl_web_to_gcs,
     name="github-flow",
     storage=github_block,
     entrypoint="02_week/etl_web_to_gcs.py:etl_web_to_gcs")

if __name__ == "__main__":
    github_deploy.apply()
