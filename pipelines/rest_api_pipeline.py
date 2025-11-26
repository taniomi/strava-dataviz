# Databricks notebook source  # noqa: INP001
"""This is a REST API pipeline for extracting data from Strava API."""
# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# DBTITLE 1,Imports
import logging
from typing import Any

import dlt
from dlt.sources.rest_api import (
    RESTAPIConfig,
    rest_api_resources,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# omit databricks message "Received command c on object id p0"
logging.getLogger("py4j").setLevel(logging.ERROR)


# COMMAND ----------

# MAGIC %md
# MAGIC # Functions and definitions

# COMMAND ----------

# DBTITLE 1,Define pipeline

# Define Strava API source in dlt
@dlt.source(name="strava")
def strava_api_source(api_token: str = dlt.secrets.value) -> Any:  # noqa: ANN401
    """Create a REST API configuration for the Strava API."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://www.strava.com/api/v3/",
            "auth": {
                "type": "bearer",
                "token": api_token,
            },
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
        },
        "resources": [ # here we define the endpoints
            {
                "name": "athlete",
                "table_name": "athlete", # optional
                "endpoint": {
                    "path": "athlete",
                    "method": "GET",
                    "paginator": "single_page"
                },
                "processing_steps": [ # filter and transform data
                ],
            },
        ],
    }
    yield from rest_api_resources(config) # understand what is this `yield from`

# Define Strava pipeline
def load_strava() -> None:
    """Load Strava data into Databricks."""
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_strava",
        destination="databricks",
        dataset_name="strava_dataviz",
    )

    load_info = pipeline.run(strava_api_source())
    print(load_info)  # noqa: T201

# COMMAND ----------

# MAGIC %md
# MAGIC # Main

# COMMAND ----------

# DBTITLE 1,Run pipeline
load_strava()

# COMMAND ----------
