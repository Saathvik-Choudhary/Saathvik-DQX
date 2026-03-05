# Databricks notebook source
import dlt

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Create Lakeflow Pipeline (formerly Delta Live Tables - DLT) Test
# MAGIC
# MAGIC Create new ETL Pipeline to execute this notebook (see [here](https://docs.databricks.com/aws/en/getting-started/data-pipeline-get-started)):
# MAGIC 1. Upload the notebook to a Databricks Workspace
# MAGIC 2. Go to `Workflows` tab > `Create` > `ETL Pipeline` > `Add existing assets` > select the source code path and root directory
# MAGIC 3. Add DQX library as a [dependency](https://docs.databricks.com/aws/en/dlt/dlt-multi-file-editor#environment) to the pipeline: Go to `Settings` > `Edit environment` > Add `databricks‑labs‑dqx` as dependency
# MAGIC 4. Run the pipeline
# MAGIC
# MAGIC DQX is installed at the cluster or pipeline level for these tests.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Lakeflow Pipeline

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

# COMMAND ----------

@dlt.view
def bronze():
  tips_url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv"
  df = (
    spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(tips_url)
  )
  return df

# COMMAND ----------

# Define Data Quality checks
import yaml

# Define checks in YAML format. They can also be defined using classes or loaded from a file or table.
checks = yaml.safe_load("""
- check:
    function: is_not_null
    arguments:
      column: total_bill
  name: total_bill_is_null
  criticality: error
- check:
    function: is_not_null
    arguments:
      column: tip
  name: tip_is_null
  criticality: error

- check:
    function: is_in_range
    arguments:
      column: total_bill
      min_limit: 1
      max_limit: 100
  name: total_bill_not_in_range
  criticality: warn

- check:
    function: is_not_less_than
    arguments:
      column: tip
      limit: 0
  name: tip_not_less_than_zero
  criticality: warn

- check:
    function: is_in_list
    arguments:
      column: day
      allowed:
        - Sun
        - Sat
        - Thur
        - Fri
  name: day_is_not_valid
  criticality: warn

- check:
    function: is_in_range
    arguments:
      column: size
      min_limit: 1
      max_limit: 6
  name: party_size_not_in_range
  criticality: warn
""")

# COMMAND ----------

dq_engine = DQEngine(WorkspaceClient())

# Read data from Bronze and apply checks
@dlt.view
def bronze_dq_check():
  df = dlt.read("bronze")
  return dq_engine.apply_checks_by_metadata(df, checks)

# COMMAND ----------

# # get rows without errors or warnings, and drop auxiliary columns
@dlt.table
def silver():
  df = dlt.read("bronze_dq_check")
  return dq_engine.get_valid(df)

# COMMAND ----------

# get only rows with errors or warnings
@dlt.table
def quarantine():
  df = dlt.read("bronze_dq_check")
  return dq_engine.get_invalid(df)