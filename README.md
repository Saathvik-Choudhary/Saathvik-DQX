# DQX Project

This project scaffolds a full Databricks/Python DQX workflow based on `Notebooks.txt`.

## Structure

- `notebooks/`: Primary DQX notebooks, grouped by lifecycle stage
- `notebooks/_shared/env.py`: Environment helpers used via `%run`

## Environment Setup (dev/test/prod)

Set these environment variables per workspace/cluster:

- `DQX_ENV` = `dev` | `test` | `prod`
- `DQX_CATALOG`
- `DQX_RULES_SCHEMA`
- `DQX_METRICS_SCHEMA`
- `DQX_QUARANTINE_SCHEMA`
- `DQX_RULES_TABLE`
- `DQX_METRICS_TABLE`
- `DQX_QUARANTINE_TABLE`

## Execution Order

1. Bootstrap & Configuration
   - `notebooks/01_bootstrap_config/NB-01_platform_bootstrap.py`
   - `notebooks/01_bootstrap_config/NB-02_dqx_installation_validation.py`
2. Profiling & Rule Discovery
   - `notebooks/02_profiling_rule_discovery/NB-03_data_profiling.py`
   - `notebooks/02_profiling_rule_discovery/NB-04_rule_generation_non_ai.py`
   - `notebooks/02_profiling_rule_discovery/NB-05_ai_rule_generation.py`
   - `notebooks/02_profiling_rule_discovery/NB-06_ai_primary_key_detection.py`
3. Data Contract Integration
   - `notebooks/03_data_contract_integration/NB-07_data_contract_to_dq_rules.py`
4. Quality Execution
   - `notebooks/04_quality_execution/NB-08_in_transit_quality_checks_batch.py`
   - `notebooks/04_quality_execution/NB-09_in_transit_quality_checks_streaming.py`
   - `notebooks/04_quality_execution/NB-10_at_rest_quality_checks.py`
   - `notebooks/04_quality_execution/NB-11_dlt_expectations_generator.py`
5. Metrics, Scoring & Analysis
   - `notebooks/05_metrics_scoring_analysis/NB-12_summary_metrics_aggregation.py`
   - `notebooks/05_metrics_scoring_analysis/NB-13_data_quality_scoring.py`
   - `notebooks/05_metrics_scoring_analysis/NB-14_anomaly_detection_on_quality_metrics.py`
6. Governance, Promotion & Ops
   - `notebooks/06_governance_promotion_ops/NB-15_rule_lifecycle_management.py`
   - `notebooks/06_governance_promotion_ops/NB-16_dev_test_prod_promotion.py`
   - `notebooks/06_governance_promotion_ops/NB-17_ci_synthetic_data_tests.py`
   - `notebooks/06_governance_promotion_ops/NB-18_schema_drift_detection.py`

