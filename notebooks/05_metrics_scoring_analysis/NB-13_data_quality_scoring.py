# Databricks notebook source
# MAGIC %md
# MAGIC # NB-13: Data Quality Scoring
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Create business-readable scores.
# MAGIC
# MAGIC **Responsibilities**
# MAGIC
# MAGIC - Compute per-table scores
# MAGIC - Domain-level rollups
# MAGIC - Weight rule severity
# MAGIC
# MAGIC **Runs**
# MAGIC
# MAGIC Scheduled (daily).

# COMMAND ----------
# MAGIC %run ../_shared/env

# COMMAND ----------
metrics_table = get_metrics_table()

# COMMAND ----------
# TODO: Compute data quality scores from metrics_table.
# - Define weighting and severity model
# - Persist per-table and domain rollups

