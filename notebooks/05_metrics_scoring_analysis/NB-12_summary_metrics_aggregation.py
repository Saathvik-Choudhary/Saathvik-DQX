# Databricks notebook source
# MAGIC %md
# MAGIC # NB-12: Summary Metrics Aggregation
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Normalize metrics across runs.
# MAGIC
# MAGIC **Responsibilities**
# MAGIC
# MAGIC - Aggregate DQX metrics
# MAGIC - Compute failure rates
# MAGIC - Persist time-series metrics
# MAGIC
# MAGIC **Runs**
# MAGIC
# MAGIC After each quality job.

# COMMAND ----------
# MAGIC %run ../_shared/env

# COMMAND ----------
metrics_table = get_metrics_table()

# COMMAND ----------
# TODO: Load raw metrics and aggregate into time-series summary tables.
# Example outline:
# metrics_df = spark.table(metrics_table)
# summary_df = ...
# summary_df.write.mode("append").saveAsTable("...")

