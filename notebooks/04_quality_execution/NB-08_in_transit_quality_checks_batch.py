# Databricks notebook source
# MAGIC %md
# MAGIC # NB-08: In-Transit Quality Checks (Batch)
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Validate data before write.
# MAGIC
# MAGIC **Responsibilities**
# MAGIC
# MAGIC - Read raw input
# MAGIC - Apply DQEngine.apply_checks_and_split
# MAGIC - Write valid / quarantine outputs
# MAGIC - Emit metrics
# MAGIC
# MAGIC **Runs**
# MAGIC
# MAGIC Every batch pipeline.

# COMMAND ----------
# MAGIC %run ../_shared/env

# COMMAND ----------
dbutils.widgets.text("input_table", "", "Input Table (catalog.schema.table)")
dbutils.widgets.text("valid_output_table", "", "Valid Output Table")
dbutils.widgets.text("quarantine_output_table", "", "Quarantine Output Table")

input_table = dbutils.widgets.get("input_table").strip()
valid_output_table = dbutils.widgets.get("valid_output_table").strip()
quarantine_output_table = dbutils.widgets.get("quarantine_output_table").strip()

if not input_table or not valid_output_table or not quarantine_output_table:
    raise ValueError("input_table, valid_output_table, and quarantine_output_table are required")

# COMMAND ----------
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
dq_engine = DQEngine(ws, spark)

# COMMAND ----------
df = spark.table(input_table)

# COMMAND ----------
# TODO: Load rules from your rules store and run apply_checks_and_split.
# Example outline (verify config APIs for your DQX version):
# checks = dq_engine.load_checks(get_rules_table())
# valid_df, quarantine_df, metrics_df = dq_engine.apply_checks_and_split(df, checks)

# COMMAND ----------
# TODO: Write outputs and metrics to target tables.

