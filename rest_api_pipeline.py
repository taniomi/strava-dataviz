# Databricks notebook source
# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# DBTITLE 1,Install dlt
# MAGIC %pip install dlt[databricks]>=1.18.2

# COMMAND ----------

# DBTITLE 1,Restart kernel
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Import dlt
import sys

# dlt patching hook is the first one on the list
metas = list(sys.meta_path)
sys.meta_path = metas[1:]

import dlt
sys.meta_path = metas  # restore post import hooks

# COMMAND ----------

# DBTITLE 1,Imports
import logging
from typing import Any, Optional

import dlt
from dlt.common.pendulum import pendulum
from dlt.destinations import databricks
from dlt.sources.rest_api import (
    RESTAPIConfig,
    check_connection,
    rest_api_resources,
    rest_api_source,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# omit databricks message "Received command c on object id p0"
logging.getLogger("py4j").setLevel(logging.ERROR)


# COMMAND ----------

# MAGIC %md
# MAGIC # Functions and definitions

# COMMAND ----------

# DBTITLE 1,Define Strava API source in dlt
@dlt.source(name="strava")
def strava_api_source(api_key: str = dlt.secrets.value) -> Any:
    """Create a REST API configuration for the Strava API."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://www.strava.com/api/v3/",
            "auth": {
                "type": "bearer",
                "token": api_key,
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
            }
        ],
    }
    yield from rest_api_resources(config) # understand what is this `yield from`

# COMMAND ----------

# DBTITLE 1,Define Strava pipeline
def load_strava() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_strava",
        destination="databricks",
        dataset_name="rest_api_data",
    )

    load_info = pipeline.run(strava_api_source())
    print(load_info)  # noqa: T201

# COMMAND ----------

# MAGIC %md
# MAGIC # Main

# COMMAND ----------

# DBTITLE 1,Run pipeline
load_strava()
