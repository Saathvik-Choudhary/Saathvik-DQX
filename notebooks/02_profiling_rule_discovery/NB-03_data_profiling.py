# Databricks notebook source
# MAGIC %md
# MAGIC # NB-03: Data Profiling
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Understand data statistically.
# MAGIC
# MAGIC **Responsibilities**
# MAGIC
# MAGIC - Run DQProfiler on tables or DataFrames
# MAGIC - Store profile results
# MAGIC - Support sampling & limits
# MAGIC
# MAGIC **Runs**
# MAGIC
# MAGIC On new datasets; periodically (weekly / monthly).

# COMMAND ----------
# MAGIC %run ../_shared/env

# COMMAND ----------
dbutils.widgets.text("input_table", "", "Input Table (catalog.schema.table)")
dbutils.widgets.text("row_limit", "", "Row Limit (optional)")

input_table = dbutils.widgets.get("input_table").strip()
row_limit_raw = dbutils.widgets.get("row_limit").strip()

if not input_table:
    raise ValueError("input_table is required")

row_limit = int(row_limit_raw) if row_limit_raw else None

# COMMAND ----------
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.config import InputConfig
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
profiler = DQProfiler(ws)

# COMMAND ----------
input_cfg = InputConfig(location=input_table, limit=row_limit)
summary_stats, profiles = profiler.profile_table(input_cfg)

# COMMAND ----------
display(summary_stats)

# COMMAND ----------
# TODO: Persist profiling output to your chosen storage (UC table or files).

