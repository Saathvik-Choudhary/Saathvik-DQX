# Databricks notebook source
# MAGIC %md
# MAGIC # NB-10: At-Rest Quality Checks
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Validate persisted data.
# MAGIC
# MAGIC **Responsibilities**
# MAGIC
# MAGIC - Read Delta tables
# MAGIC - Apply rules via metadata
# MAGIC - Write validation results
# MAGIC - Store metrics
# MAGIC
# MAGIC **Runs**
# MAGIC
# MAGIC Scheduled (hourly / daily).

# COMMAND ----------
# MAGIC %run ../_shared/env

# COMMAND ----------
dbutils.widgets.text("target_table", "", "Target Table (catalog.schema.table)")
target_table = dbutils.widgets.get("target_table").strip()
if not target_table:
    raise ValueError("target_table is required")

# COMMAND ----------
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
dq_engine = DQEngine(ws, spark)

# COMMAND ----------
df = spark.table(target_table)

# COMMAND ----------
# TODO: Load rules for target_table and execute validation.
# Persist results and metrics to your metrics store.

