# Databricks notebook source
# MAGIC %md
# MAGIC # NB-09: In-Transit Quality Checks (Streaming)
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Streaming-safe quality enforcement.
# MAGIC
# MAGIC **Responsibilities**
# MAGIC
# MAGIC - Apply streaming-compatible DQ rules
# MAGIC - Handle late data
# MAGIC - Write valid + quarantine streams
# MAGIC
# MAGIC **Runs**
# MAGIC
# MAGIC Continuous.

# COMMAND ----------
# MAGIC %run ../_shared/env

# COMMAND ----------
dbutils.widgets.text("input_stream_table", "", "Input Stream Table")
dbutils.widgets.text("valid_output_table", "", "Valid Output Table")
dbutils.widgets.text("quarantine_output_table", "", "Quarantine Output Table")
dbutils.widgets.text("checkpoint_path", "", "Checkpoint Path")

input_stream_table = dbutils.widgets.get("input_stream_table").strip()
valid_output_table = dbutils.widgets.get("valid_output_table").strip()
quarantine_output_table = dbutils.widgets.get("quarantine_output_table").strip()
checkpoint_path = dbutils.widgets.get("checkpoint_path").strip()

if not input_stream_table or not valid_output_table or not quarantine_output_table or not checkpoint_path:
    raise ValueError("All inputs are required for streaming checks")

# COMMAND ----------
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
dq_engine = DQEngine(ws, spark)

# COMMAND ----------
stream_df = spark.readStream.table(input_stream_table)

# COMMAND ----------
# TODO: Apply streaming-compatible DQ checks and write to valid/quarantine streams.
# Ensure exactly-once semantics and watermarking as required.

