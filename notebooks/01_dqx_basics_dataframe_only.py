# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1: DQX Basic Features (DataFrame-only)
# MAGIC
# MAGIC This notebook explores **profiling**, **DQEngine**, and **DQ checks** using only DataFrames.
# MAGIC - **Input:** DataFrames  
# MAGIC - **Output:** All results and intermediates as DataFrames (no Unity Catalog or external storage)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and sample input DataFrame
# MAGIC
# MAGIC Create a WorkspaceClient (uses Databricks notebook auth) and a sample DataFrame as input.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()
ws = WorkspaceClient()

# Sample input data as DataFrame (replace with your own DataFrame in practice)
input_data = [
    Row(id=1, name="Alice", age=30, city="NYC", amount=100.50),
    Row(id=2, name="Bob", age=25, city="LA", amount=200.00),
    Row(id=3, name="Charlie", age=None, city="NYC", amount=150.00),  # null age
    Row(id=4, name="", age=35, city="SF", amount=75.25),               # empty name
    Row(id=5, name="Eve", age=40, city="LA", amount=300.00),
]
input_df = spark.createDataFrame(input_data)
display(input_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Profiling (DataFrame in → summary stats + profiles)
# MAGIC
# MAGIC Profile the DataFrame to get summary statistics and suggested DQ rules (profiles).  
# MAGIC We then store **summary stats** and **profiles** as DataFrames.

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import DQProfiler

profiler = DQProfiler(workspace_client=ws, spark=spark)
# Profile using DataFrame (no table path)
summary_stats, profiles = profiler.profile(input_df, options={"limit": 1000, "sample_fraction": 1.0})

print(f"Profiled {len(profiles)} rule candidates.")
print("Summary stats keys:", list(summary_stats.keys())[:5], "...")

# COMMAND ----------

# Store summary statistics as a DataFrame (column name + stats as string for display)
summary_stats_df = spark.createDataFrame([Row(column=k, stats=str(v)) for k, v in summary_stats.items()])
display(summary_stats_df)

# COMMAND ----------

# Store profiles (rule candidates) as a DataFrame
from pyspark.sql.types import StructType, StructField, StringType

profile_rows = [
    Row(name=p.name, column=p.column, description=p.description or "", filter=p.filter or "", parameters=str(p.parameters or {}))
    for p in profiles
]
profiles_schema = StructType([
    StructField("name", StringType()), StructField("column", StringType()),
    StructField("description", StringType()), StructField("filter", StringType()), StructField("parameters", StringType())
])
profiles_df = spark.createDataFrame(profile_rows, profiles_schema) if profile_rows else spark.createDataFrame([], profiles_schema)
display(profiles_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate DQ rules (checks) from profiles
# MAGIC
# MAGIC Use `DQGenerator` to turn profiles into a list of check definitions, then store checks as a DataFrame.

# COMMAND ----------

from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.checks_serializer import DataFrameConverter

generator = DQGenerator(workspace_client=ws, spark=spark)
checks_list = generator.generate_dq_rules(profiles=profiles, criticality="error")

# Store checks as a DataFrame (intermediate output)
checks_df = DataFrameConverter.to_dataframe(spark, checks_list, run_config_name="default")
display(checks_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate checks
# MAGIC
# MAGIC Validate the generated checks before applying them.

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine

status = DQEngine.validate_checks(checks_list)
print("Validation has errors:", status.has_errors)
if status.has_errors:
    print("Errors:", status.errors)
else:
    print("All checks are valid.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. DQEngine: apply checks (DataFrame in → DataFrame out)
# MAGIC
# MAGIC Apply the checks to the input DataFrame. Result is a new DataFrame with error/warning columns.

# COMMAND ----------

engine = DQEngine(workspace_client=ws, spark=spark)
result_df = engine.apply_checks_by_metadata(input_df, checks_list)
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Apply checks and split into good/bad DataFrames
# MAGIC
# MAGIC Get two DataFrames: rows passing all checks vs rows with errors/warnings.

# COMMAND ----------

good_df, bad_df = engine.apply_checks_by_metadata_and_split(input_df, checks_list)
print("Good rows:", good_df.count())
print("Bad rows (errors/warnings):", bad_df.count())
display(bad_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Optional: metrics as DataFrame
# MAGIC
# MAGIC Use an observer to collect summary metrics and convert them to a DataFrame.

# COMMAND ----------

from databricks.labs.dqx.metrics_observer import DQMetricsObserver, DQMetricsObservation
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.reporting_columns import DefaultColumnNames

observer = DQMetricsObserver(name="dqx_notebook")
engine_with_observer = DQEngine(workspace_client=ws, spark=spark, extra_params=None, observer=observer)
result_with_obs = engine_with_observer.apply_checks_by_metadata(input_df, checks_list)

# result_with_obs is (df, observation) when observer is set; observation is Spark's Observation
if isinstance(result_with_obs, tuple):
    result_df_observed, observation = result_with_obs
    result_df_observed.count()  # trigger computation so observation.get() is populated
    metrics_observation = DQMetricsObservation(
        run_id=engine_with_observer._engine.run_id,
        run_name=observer.name,
        error_column_name=DefaultColumnNames.ERRORS.value,
        warning_column_name=DefaultColumnNames.WARNINGS.value,
        run_time_overwrite=None,
        observed_metrics=observation.get(),
        input_location="", output_location="", quarantine_location="", checks_location="",
        user_metadata=None,
    )
    metrics_df = DQMetricsObserver.build_metrics_df(spark, metrics_observation)
    display(metrics_df)
else:
    result_with_obs.count()
    print("No observer; metrics DataFrame skipped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **Input:** `input_df` (DataFrame)  
# MAGIC - **Intermediates (DataFrames):** `summary_stats_df`, `profiles_df`, `checks_df`  
# MAGIC - **Outputs (DataFrames):** `result_df` (with error/warning columns), `good_df`, `bad_df`, optionally `metrics_df`  
# MAGIC All steps use only DataFrames; no Unity Catalog or external storage.
