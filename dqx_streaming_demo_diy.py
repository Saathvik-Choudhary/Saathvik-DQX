# Databricks notebook source
# MAGIC %md
# MAGIC ### DQX Structured Streaming Test
# MAGIC
# MAGIC DQX is installed at the cluster level. This notebook validates the structured streaming path.

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Data Quality Checks
# MAGIC
# MAGIC This cell defines a set of data quality checks using YAML syntax. The checks are designed to validate key columns in the open-source `tips` dataset, such as `total_bill`, `tip`, `day`, and `size`. Each check specifies a function, its arguments, a name, and a criticality level (`error` or `warn`). These checks will be used by the `DQEngine` to enforce data quality rules on the streaming data.

# COMMAND ----------

# Define Data Quality checks
import yaml


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

# MAGIC %md
# MAGIC ### Inputs
# MAGIC Checkpoint and table locations are set via widgets.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h3>Checkpoint and Table Location Options</h3>
# MAGIC <ul>
# MAGIC   <li>
# MAGIC     <b>Checkpoint Location can be specified as:</b>
# MAGIC     <ul>
# MAGIC       <li>Local path (e.g., <code>/tmp/checkpoints/...</code>)</li>
# MAGIC       <li>Workspace path (e.g., <code>/dbfs/mnt/...</code>)</li>
# MAGIC       <li>Unity Catalog (UC) volume path (e.g., <code>/Volumes/catalog/schema/volume/...</code>)</li>
# MAGIC     </ul>
# MAGIC   </li>
# MAGIC   <li>
# MAGIC     <b>Silver and Quarantine Table</b>:
# MAGIC     <ul>
# MAGIC       <li>Unity Catalog table (e.g., <code>catalog.schema.table</code>)</li>
# MAGIC     </ul>
# MAGIC   </li>
# MAGIC </ul>

# COMMAND ----------

dbutils.widgets.text("silver_checkpoint", "/tmp/dq/structured/bronze_to_silver_checkpoint")
dbutils.widgets.text("quarantine_checkpoint", "/tmp/dq/structured/bronze_to_quarantine_checkpoint")
dbutils.widgets.text("silver_table", "/tmp/dq/structured/silver_table")
dbutils.widgets.text("quarantine_table", "/tmp/dq/structured/quarantine_table")

# COMMAND ----------

silver_checkpoint = dbutils.widgets.get("silver_checkpoint")
quarantine_checkpoint = dbutils.widgets.get("quarantine_checkpoint")
silver_table = dbutils.widgets.get("silver_table")
quarantine_table = dbutils.widgets.get("quarantine_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Stream
# MAGIC
# MAGIC This cell reads the open-source tips dataset from a Delta table as a streaming DataFrame, which will be used as the input for downstream data quality checks and transformations.

# COMMAND ----------

tips_url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv"
bronze_path = "/tmp/dqx/streaming_tips_bronze"

bronze_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(tips_url)
)
bronze_df.write.mode("overwrite").format("delta").save(bronze_path)

bronze_stream = spark.readStream.format("delta").load(bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply data quality checks and split the bronze data into silver and quarantine DataFrames

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

dq_engine = DQEngine(WorkspaceClient())

silver_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(bronze_stream, checks)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write streaming DataFrames to Delta tables with checkpointing

# COMMAND ----------

PATH_PREFIXES = ("/", "dbfs:/", "s3://", "abfss://", "gs://")

def write_stream(df, checkpoint_location, target):
    writer = (
        df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location)
            .trigger(availableNow=True)  # stop the stream once all data is processed
    )
    if target.startswith(PATH_PREFIXES):
        return writer.start(target)
    else:
        return writer.toTable(target)

silver_query = write_stream(silver_df, silver_checkpoint, silver_table)
quarantine_query = write_stream(quarantine_df, quarantine_checkpoint, quarantine_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wait for the streams to finish or keep them running for interactive sessions

# COMMAND ----------

silver_query.awaitTermination()
quarantine_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display Results

# COMMAND ----------

display(spark.sql(f"SELECT * FROM delta.`{silver_table}`"))
display(spark.sql(f"SELECT * FROM delta.`{quarantine_table}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Cleanup

# COMMAND ----------

dbutils.fs.rm(silver_checkpoint, True)
dbutils.fs.rm(quarantine_checkpoint, True)
dbutils.fs.rm(silver_table, True)
dbutils.fs.rm(quarantine_table, True)
spark.sql("DROP TABLE IF EXISTS silver_table")
spark.sql("DROP TABLE IF EXISTS quarantine_table")