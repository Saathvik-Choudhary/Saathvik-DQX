# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2: DQX AI, Anomaly & LLM Features (DataFrame-only)
# MAGIC
# MAGIC This notebook explores **AI-assisted rule generation**, **anomaly detection**, and **LLM features** using DataFrames.
# MAGIC - **Input:** DataFrames  
# MAGIC - **Output:** All results as DataFrames  
# MAGIC
# MAGIC **Note:** Anomaly detection requires the `anomaly` extra: `pip install 'databricks-labs-dqx[anomaly]'`  
# MAGIC **Note:** LLM/AI features require the `llm` extra: `pip install 'databricks-labs-dqx[llm]'`  
# MAGIC **Anomaly:** The library persists model metadata to a Delta table (registry). We use the Spark **default** database to avoid Unity Catalog; no other storage is used for inputs/outputs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and sample input DataFrame
# MAGIC
# MAGIC Create WorkspaceClient and a sample DataFrame. For AI/LLM we use profile summary stats derived from this DataFrame.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()
ws = WorkspaceClient()

# Sample input (replace with your DataFrame in practice)
input_data = [
    Row(id=1, region="US", amount=100.0, count=5),
    Row(id=2, region="EU", amount=250.0, count=12),
    Row(id=3, region="US", amount=80.0, count=3),
    Row(id=4, region="APAC", amount=500.0, count=20),
    Row(id=5, region="US", amount=30.0, count=1),
    Row(id=6, region="EU", amount=120.0, count=8),
]
input_df = spark.createDataFrame(input_data)
display(input_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. LLM / AI-assisted rule generation (DataFrame → profile stats → LLM → checks DataFrame)
# MAGIC
# MAGIC Profile the DataFrame to get summary stats, then use the **LLM** to generate quality rules from natural language + stats.  
# MAGIC Requires: `pip install 'databricks-labs-dqx[llm]'`

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.checks_serializer import DataFrameConverter

# Profile to get summary_stats (from DataFrame only)
profiler = DQProfiler(workspace_client=ws, spark=spark)
summary_stats, profiles = profiler.profile(input_df, options={"limit": 1000, "sample_fraction": 1.0})

# Generate rules using LLM: pass summary_stats + optional natural language
generator = DQGenerator(workspace_client=ws, spark=spark)
user_requirements = "Ensure amount and count are positive; region should be one of US, EU, APAC."
ai_checks_list = generator.generate_dq_rules_ai_assisted(
    user_input=user_requirements,
    summary_stats=summary_stats,
    input_config=None,  # no table – we only use summary_stats from the DataFrame above
)

# Store AI-generated checks as a DataFrame
ai_checks_df = DataFrameConverter.to_dataframe(spark, ai_checks_list, run_config_name="default")
display(ai_checks_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Apply AI-generated checks with DQEngine (DataFrame in → DataFrame out)
# MAGIC
# MAGIC Apply the LLM-generated checks to the input DataFrame. Results are DataFrames only.

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine

engine = DQEngine(workspace_client=ws, spark=spark)
result_ai_df = engine.apply_checks_by_metadata(input_df, ai_checks_list)
display(result_ai_df)

good_ai_df, bad_ai_df = engine.apply_checks_by_metadata_and_split(input_df, ai_checks_list)
print("Good rows:", good_ai_df.count(), "Bad rows:", bad_ai_df.count())
display(bad_ai_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. LLM primary key detection (DataFrame only)
# MAGIC
# MAGIC Use a temporary view created from the DataFrame so the LLM can suggest primary keys (no Unity Catalog table).  
# MAGIC Requires: `pip install 'databricks-labs-dqx[llm]'`

# COMMAND ----------

from databricks.labs.dqx.config import InputConfig
from databricks.labs.dqx.profiler.profiler import DQProfiler
import uuid

# Create a temp view from our DataFrame so PK detection has a "table" to analyze
temp_view = f"temp_pk_{uuid.uuid4().hex[:8]}"
input_df.createOrReplaceTempView(temp_view)

profiler_pk = DQProfiler(workspace_client=ws, spark=spark)
pk_result = profiler_pk.detect_primary_keys_with_llm(InputConfig(location=temp_view))

# Store PK result as a DataFrame
pk_rows = [Row(table=pk_result.get("table", ""), success=pk_result.get("success", False), result=str(pk_result))]
pk_df = spark.createDataFrame(pk_rows)
display(pk_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Anomaly detection – train (DataFrame in)
# MAGIC
# MAGIC Train an anomaly model on the input DataFrame. The library stores model metadata in a **registry table**; we use the Spark **default** database so no Unity Catalog is required.  
# MAGIC Requires: `pip install 'databricks-labs-dqx[anomaly]'`

# COMMAND ----------

from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine

# Use default Spark database for registry (no Unity Catalog)
REGISTRY_TABLE = "default.dqx_anomaly_registry"
MODEL_NAME = "default.dqx_anomaly_model"

anomaly_engine = AnomalyEngine(workspace_client=ws, spark=spark)
model_name = anomaly_engine.train(
    df=input_df,
    model_name=MODEL_NAME,
    registry_table=REGISTRY_TABLE,
    columns=["amount", "count"],  # optional: omit for auto-discovery
    segment_by=None,
)
print("Trained model:", model_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Anomaly detection – score (DataFrame in → DataFrame out)
# MAGIC
# MAGIC Score the same or another DataFrame with the trained model. Output is a DataFrame with anomaly scores.

# COMMAND ----------

from databricks.labs.dqx.anomaly.scoring_orchestrator import run_anomaly_scoring
from databricks.labs.dqx.anomaly.scoring_config import ScoringConfig
from databricks.labs.dqx.reporting_columns import DefaultColumnNames

config = ScoringConfig(
    columns=["amount", "count"],
    model_name=MODEL_NAME,
    registry_table=REGISTRY_TABLE,
    threshold=0.5,
    merge_columns=["id"],  # join key to merge scores back to input rows
    segment_by=None,
)

scored_df = run_anomaly_scoring(
    df_to_score=input_df,
    config=config,
    registry_table=REGISTRY_TABLE,
    model_name=MODEL_NAME,
)

# Result is a DataFrame with anomaly scores (and optional severity/contributions)
display(scored_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Store anomaly results as DataFrames (e.g. high-risk rows)
# MAGIC
# MAGIC Split scored output into “normal” vs “anomalous” based on a threshold and keep everything as DataFrames.

# COMMAND ----------

from pyspark.sql import functions as F

# Example: flag rows above a score threshold (column name may vary; check scored_df columns)
score_col = "anomaly_score"
if score_col in scored_df.columns:
    threshold_value = 0.6
    normal_df = scored_df.filter(F.col(score_col) <= threshold_value)
    anomalous_df = scored_df.filter(F.col(score_col) > threshold_value)
    print("Normal rows:", normal_df.count(), "Anomalous rows:", anomalous_df.count())
    display(anomalous_df)
else:
    print("Score column not found. Columns:", scored_df.columns)
    display(scored_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **AI/LLM:** Profile → `summary_stats` → `generate_dq_rules_ai_assisted()` → `ai_checks_df`; apply checks → `result_ai_df`, `good_ai_df`, `bad_ai_df`.  
# MAGIC - **LLM PK detection:** DataFrame → temp view → `detect_primary_keys_with_llm` → `pk_df`.  
# MAGIC - **Anomaly:** Train on `input_df` with registry in `default` DB → score `input_df` → `scored_df`; split into `normal_df` / `anomalous_df`.  
# MAGIC All inputs and outputs are DataFrames except the anomaly model registry table (required by the library; we use the default database only).
