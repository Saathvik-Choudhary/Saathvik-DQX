# Databricks notebook source
# MAGIC %md
# MAGIC # NB-14: Anomaly Detection on Quality Metrics
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Detect unknown failures.
# MAGIC
# MAGIC **Responsibilities**
# MAGIC
# MAGIC - Run statistical / ML models
# MAGIC - Detect spikes & drifts
# MAGIC - Persist anomaly flags
# MAGIC
# MAGIC **Runs**
# MAGIC
# MAGIC Daily / hourly.

# COMMAND ----------
# MAGIC %run ../_shared/env

# COMMAND ----------
metrics_table = get_metrics_table()

# COMMAND ----------
# TODO: Implement anomaly detection on metrics_table and persist flags.

