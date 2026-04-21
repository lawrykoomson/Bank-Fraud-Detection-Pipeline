"""
Apache Airflow DAG — Bank Fraud Detection Pipeline
===================================================
Schedules the fraud detection pipeline to run every
day at 02:00 AM UTC (overnight batch processing).

Tasks:
    1. extract_transactions  — generate/load bank transactions
    2. score_fraud_risk      — engineer features + score fraud
    3. load_to_postgres      — load into PostgreSQL warehouse
    4. refresh_dbt_models    — rebuild analytical layer
    5. notify_compliance     — log alerts summary

Author: Lawrence Koomson
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logger = logging.getLogger(__name__)

default_args = {
    "owner":            "lawrence_koomson",
    "depends_on_past":  False,
    "email":            ["koomsonlawrence64@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

dag = DAG(
    dag_id="bank_fraud_detection_pipeline",
    default_args=default_args,
    description="Daily fraud detection pipeline for Absa Bank Ghana transactions",
    schedule_interval="0 2 * * *",   # Every day at 02:00 AM UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["fraud", "banking", "absa", "ghana", "compliance", "data-engineering"],
)


def task_extract(**context):
    from fraud_pipeline import extract
    df = extract()
    temp_path = "/tmp/fraud_raw.csv"
    df.to_csv(temp_path, index=False)
    context["ti"].xcom_push(key="raw_count", value=len(df))
    context["ti"].xcom_push(key="temp_path", value=temp_path)
    logger.info(f"Extracted {len(df):,} transactions")
    return len(df)


def task_score(**context):
    import pandas as pd
    from fraud_pipeline import transform
    temp_path = context["ti"].xcom_pull(task_ids="extract_transactions", key="temp_path")
    df = pd.read_csv(temp_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    result = transform(df)
    clean_path = "/tmp/fraud_scored.csv"
    result.to_csv(clean_path, index=False)
    flagged  = int(result["requires_review"].sum())
    critical = int((result["fraud_risk_tier"] == "Critical").sum())
    value    = float(result[result["requires_review"]]["amount_ghs"].sum())
    context["ti"].xcom_push(key="clean_path",     value=clean_path)
    context["ti"].xcom_push(key="flagged_count",  value=flagged)
    context["ti"].xcom_push(key="critical_count", value=critical)
    context["ti"].xcom_push(key="flagged_value",  value=value)
    logger.info(f"Scored {len(result):,} transactions | Flagged: {flagged:,}")
    return flagged


def task_load(**context):
    import pandas as pd
    from fraud_pipeline import transform, load
    temp_path = context["ti"].xcom_pull(task_ids="extract_transactions", key="temp_path")
    df = pd.read_csv(temp_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    result = transform(df)
    load(result)
    logger.info("Load to PostgreSQL complete")
    return "success"


def task_notify(**context):
    run_date  = context["ds"]
    raw       = context["ti"].xcom_pull(task_ids="extract_transactions", key="raw_count")
    flagged   = context["ti"].xcom_pull(task_ids="score_fraud_risk",     key="flagged_count")
    critical  = context["ti"].xcom_pull(task_ids="score_fraud_risk",     key="critical_count")
    value     = context["ti"].xcom_pull(task_ids="score_fraud_risk",     key="flagged_value")

    logger.info("=" * 60)
    logger.info("  FRAUD PIPELINE — DAILY COMPLIANCE NOTIFICATION")
    logger.info("=" * 60)
    logger.info(f"  Run Date              : {run_date}")
    logger.info(f"  Transactions Scored   : {raw:,}")
    logger.info(f"  Flagged for Review    : {flagged:,}")
    logger.info(f"  Critical Risk         : {critical:,}")
    logger.info(f"  Total Value Flagged   : GHS {value:,.2f}")
    logger.info("=" * 60)
    return "notified"


start          = EmptyOperator(task_id="pipeline_start",    dag=dag)
extract_task   = PythonOperator(task_id="extract_transactions", python_callable=task_extract, dag=dag)
score_task     = PythonOperator(task_id="score_fraud_risk",     python_callable=task_score,   dag=dag)
load_task      = PythonOperator(task_id="load_to_postgres",     python_callable=task_load,    dag=dag)
notify_task    = PythonOperator(task_id="notify_compliance",    python_callable=task_notify,  dag=dag)
end            = EmptyOperator(task_id="pipeline_end",      dag=dag)

start >> extract_task >> score_task >> load_task >> notify_task >> end