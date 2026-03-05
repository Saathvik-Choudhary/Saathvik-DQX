# Databricks notebook source
# MAGIC %md
# MAGIC # NB-18: Schema Drift Detection
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Catch silent breakages.
# MAGIC
# MAGIC **Responsibilities**
# MAGIC
# MAGIC - Compare historical schemas
# MAGIC - Detect breaking vs non-breaking changes
# MAGIC - Trigger re-profiling
# MAGIC
# MAGIC **Runs**
# MAGIC
# MAGIC Scheduled.

# COMMAND ----------
# MAGIC %run ../_shared/env

# COMMAND ----------
dbutils.widgets.text("target_table", "", "Target Table (catalog.schema.table)")
target_table = dbutils.widgets.get("target_table").strip()
if not target_table:
    raise ValueError("target_table is required")

# COMMAND ----------
# TODO: Implement schema drift detection and trigger re-profiling.
print(f"Ready to check schema drift for: {target_table}")

