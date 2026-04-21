"""
Bank Transaction Fraud Detection Pipeline
==========================================
Ingests banking transaction data, engineers fraud-detection
features, scores each transaction, flags suspicious activity,
and loads into PostgreSQL for compliance reporting.

Targets: Absa Bank Ghana

Author: Lawrence Koomson
GitHub: github.com/lawrykoomson
"""

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import logging
import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("fraud_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "bank_analytics"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

PROCESSED_PATH = Path("data/processed/")

CHANNELS          = ["ATM", "Online Banking", "Mobile App", "Branch Teller", "POS Terminal"]
TRANSACTION_TYPES = ["Transfer", "Withdrawal", "Purchase", "Bill Payment", "Loan Repayment", "Deposit"]
REGIONS           = ["Greater Accra", "Ashanti", "Western", "Eastern", "Northern"]

HIGH_AMOUNT_THRESHOLD  = 5000
RAPID_TXN_WINDOW_MINS  = 5
UNUSUAL_HOUR_START     = 0
UNUSUAL_HOUR_END       = 5


def extract() -> pd.DataFrame:
    logger.info("[EXTRACT] Generating synthetic bank transaction data...")
    np.random.seed(7)
    n = 20000

    account_pool = [f"ACC{str(i).zfill(8)}" for i in range(1, 2001)]
    base_time    = datetime(2024, 1, 1)
    timestamps   = [
        base_time + timedelta(minutes=int(m))
        for m in np.random.randint(0, 525600, n)
    ]

    amounts = np.where(
        np.random.rand(n) < 0.05,
        np.random.uniform(5000, 50000, n),
        np.abs(np.random.lognormal(5, 1.2, n))
    ).round(2)

    is_fraud = np.zeros(n, dtype=int)
    fraud_idx = np.random.choice(n, int(n * 0.03), replace=False)
    is_fraud[fraud_idx] = 1

    df = pd.DataFrame({
        "transaction_id":   [f"TXN-BNK-{str(i).zfill(9)}" for i in range(1, n+1)],
        "timestamp":        timestamps,
        "sender_account":   np.random.choice(account_pool, n),
        "receiver_account": np.random.choice(account_pool, n),
        "amount_ghs":       amounts,
        "transaction_type": np.random.choice(TRANSACTION_TYPES, n,
                                p=[0.30, 0.20, 0.25, 0.10, 0.08, 0.07]),
        "channel":          np.random.choice(CHANNELS, n,
                                p=[0.20, 0.30, 0.30, 0.10, 0.10]),
        "region":           np.random.choice(REGIONS, n,
                                p=[0.35, 0.25, 0.18, 0.12, 0.10]),
        "currency":         "GHS",
        "status":           np.random.choice(
                                ["COMPLETED","PENDING","FAILED","REVERSED"], n,
                                p=[0.89, 0.05, 0.04, 0.02]),
        "device_id":        [f"DEV{np.random.randint(1000,9999)}" for _ in range(n)],
        "ip_country":       np.random.choice(
                                ["GH","NG","US","CN","RU"], n,
                                p=[0.92, 0.03, 0.02, 0.02, 0.01]),
        "is_fraud_actual":  is_fraud,
    })

    logger.info(f"[EXTRACT] Generated {len(df):,} transactions ({is_fraud.sum()} fraud injected).")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[TRANSFORM] Engineering fraud detection features...")

    df = df.sort_values(["sender_account", "timestamp"]).reset_index(drop=True)

    df["txn_hour"]        = df["timestamp"].dt.hour
    df["txn_date"]        = df["timestamp"].dt.date
    df["txn_day_of_week"] = df["timestamp"].dt.day_name()
    df["is_weekend"]      = df["timestamp"].dt.dayofweek >= 5
    df["is_unusual_hour"] = df["txn_hour"].between(UNUSUAL_HOUR_START, UNUSUAL_HOUR_END)

    df["is_high_amount"]  = df["amount_ghs"] >= HIGH_AMOUNT_THRESHOLD

    acct_mean = df.groupby("sender_account")["amount_ghs"].transform("mean")
    acct_std  = df.groupby("sender_account")["amount_ghs"].transform("std").fillna(1)
    df["amount_z_score"]    = ((df["amount_ghs"] - acct_mean) / acct_std).round(3)
    df["is_amount_outlier"] = df["amount_z_score"] > 3.0

    df["prev_timestamp"] = df.groupby("sender_account")["timestamp"].shift(1)
    df["mins_since_last_txn"] = (
        (df["timestamp"] - df["prev_timestamp"]).dt.total_seconds() / 60
    ).round(2)
    df["is_rapid_txn"] = df["mins_since_last_txn"] < RAPID_TXN_WINDOW_MINS
    df = df.drop(columns=["prev_timestamp"])

    daily_count           = df.groupby(["sender_account","txn_date"])["transaction_id"].transform("count")
    df["daily_txn_count"] = daily_count
    df["is_high_frequency"] = df["daily_txn_count"] > 10

    df["is_foreign_ip"]    = df["ip_country"] != "GH"
    df["is_self_transfer"] = df["sender_account"] == df["receiver_account"]

    df["fraud_risk_score"] = (
        df["is_high_amount"].astype(int)    * 20 +
        df["is_rapid_txn"].astype(int)      * 20 +
        df["is_amount_outlier"].astype(int) * 20 +
        df["is_unusual_hour"].astype(int)   * 15 +
        df["is_foreign_ip"].astype(int)     * 15 +
        df["is_high_frequency"].astype(int) * 10
    ).clip(0, 100)

    df["fraud_risk_tier"] = pd.cut(
        df["fraud_risk_score"],
        bins=[-1, 20, 45, 70, 100],
        labels=["Low", "Medium", "High", "Critical"]
    ).astype(str)

    df["requires_review"] = df["fraud_risk_tier"].isin(["High", "Critical"])

    def build_alert(row):
        reasons = []
        if row["is_high_amount"]:
            reasons.append(f"High amount (GHS {row['amount_ghs']:,.2f})")
        if row["is_rapid_txn"]:
            reasons.append(f"Rapid txn ({row['mins_since_last_txn']:.1f} mins after last)")
        if row["is_amount_outlier"]:
            reasons.append(f"Amount outlier (z={row['amount_z_score']:.2f})")
        if row["is_unusual_hour"]:
            reasons.append(f"Unusual hour ({row['txn_hour']:02d}:00)")
        if row["is_foreign_ip"]:
            reasons.append(f"Foreign IP ({row['ip_country']})")
        if row["is_high_frequency"]:
            reasons.append(f"High frequency ({row['daily_txn_count']} txns today)")
        return " | ".join(reasons) if reasons else "No flags"

    df["alert_reason"] = df.apply(build_alert, axis=1)
    df["processed_at"] = datetime.now()

    flagged  = df["requires_review"].sum()
    critical = (df["fraud_risk_tier"] == "Critical").sum()
    logger.info(f"[TRANSFORM] Complete. Flagged: {flagged:,} | Critical: {critical:,}")
    return df


def load(df: pd.DataFrame):
    logger.info("[LOAD] Attempting PostgreSQL connection...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)

        with conn.cursor() as cur:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS fraud_detection;

                CREATE TABLE IF NOT EXISTS fraud_detection.fact_transactions (
                    transaction_id          VARCHAR(25) PRIMARY KEY,
                    timestamp               TIMESTAMP,
                    txn_date                DATE,
                    txn_hour                SMALLINT,
                    txn_day_of_week         VARCHAR(12),
                    is_weekend              BOOLEAN,
                    sender_account          VARCHAR(15),
                    receiver_account        VARCHAR(15),
                    amount_ghs              NUMERIC(14,2),
                    transaction_type        VARCHAR(20),
                    channel                 VARCHAR(25),
                    region                  VARCHAR(50),
                    currency                VARCHAR(5),
                    status                  VARCHAR(15),
                    device_id               VARCHAR(10),
                    ip_country              VARCHAR(5),
                    is_unusual_hour         BOOLEAN,
                    is_high_amount          BOOLEAN,
                    amount_z_score          NUMERIC(8,3),
                    is_amount_outlier       BOOLEAN,
                    mins_since_last_txn     NUMERIC(10,2),
                    is_rapid_txn            BOOLEAN,
                    daily_txn_count         INT,
                    is_high_frequency       BOOLEAN,
                    is_foreign_ip           BOOLEAN,
                    is_self_transfer        BOOLEAN,
                    fraud_risk_score        NUMERIC(5,1),
                    fraud_risk_tier         VARCHAR(10),
                    requires_review         BOOLEAN,
                    alert_reason            TEXT,
                    is_fraud_actual         SMALLINT,
                    processed_at            TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS fraud_detection.alerts (
                    alert_id        SERIAL PRIMARY KEY,
                    transaction_id  VARCHAR(25),
                    risk_tier       VARCHAR(10),
                    fraud_score     NUMERIC(5,1),
                    alert_reason    TEXT,
                    amount_ghs      NUMERIC(14,2),
                    channel         VARCHAR(25),
                    region          VARCHAR(50),
                    created_at      TIMESTAMP DEFAULT NOW()
                );
            """)
            conn.commit()

        load_cols = [
            "transaction_id","timestamp","txn_date","txn_hour","txn_day_of_week",
            "is_weekend","sender_account","receiver_account","amount_ghs",
            "transaction_type","channel","region","currency","status",
            "device_id","ip_country","is_unusual_hour","is_high_amount",
            "amount_z_score","is_amount_outlier","mins_since_last_txn",
            "is_rapid_txn","daily_txn_count","is_high_frequency",
            "is_foreign_ip","is_self_transfer","fraud_risk_score",
            "fraud_risk_tier","requires_review","alert_reason",
            "is_fraud_actual","processed_at"
        ]

        records = [tuple(r) for r in df[load_cols].itertuples(index=False)]

        with conn.cursor() as cur:
            execute_values(cur,
                f"INSERT INTO fraud_detection.fact_transactions ({','.join(load_cols)}) "
                f"VALUES %s ON CONFLICT (transaction_id) DO UPDATE SET "
                f"fraud_risk_score=EXCLUDED.fraud_risk_score, "
                f"fraud_risk_tier=EXCLUDED.fraud_risk_tier, "
                f"processed_at=EXCLUDED.processed_at",
                records, page_size=500
            )

            alerts_df = df[df["requires_review"]][[
                "transaction_id","fraud_risk_tier","fraud_risk_score",
                "alert_reason","amount_ghs","channel","region"
            ]]
            if len(alerts_df):
                alert_records = [tuple(r) for r in alerts_df.itertuples(index=False)]
                execute_values(cur,
                    "INSERT INTO fraud_detection.alerts "
                    "(transaction_id,risk_tier,fraud_score,alert_reason,amount_ghs,channel,region) "
                    "VALUES %s",
                    alert_records
                )
            conn.commit()

        conn.close()
        logger.info(f"[LOAD] Successfully loaded {len(df):,} transactions + {len(alerts_df):,} alerts into PostgreSQL.")

    except Exception as e:
        logger.warning(f"[LOAD] PostgreSQL unavailable ({e})")
        logger.info("[LOAD] Falling back to CSV export...")
        fallback = PROCESSED_PATH / f"fraud_scored_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(fallback, index=False)
        logger.info(f"[LOAD] Saved to {fallback}")


def print_summary(df: pd.DataFrame):
    flagged       = df["requires_review"].sum()
    critical      = (df["fraud_risk_tier"] == "Critical").sum()
    value_flagged = df[df["requires_review"]]["amount_ghs"].sum()
    actual_fraud  = df["is_fraud_actual"].sum()

    print("\n" + "="*68)
    print("   BANK FRAUD DETECTION PIPELINE — RUN SUMMARY")
    print("="*68)
    print(f"  Total Transactions Processed : {len(df):,}")
    print(f"  Flagged for Review           : {flagged:,} ({flagged/len(df)*100:.1f}%)")
    print(f"  Critical Risk Transactions   : {critical:,}")
    print(f"  Total Value Flagged          : GHS {value_flagged:,.2f}")
    print(f"  Actual Fraud (Ground Truth)  : {actual_fraud:,} ({actual_fraud/len(df)*100:.1f}%)")
    print("-"*68)
    print("  FRAUD RISK TIER BREAKDOWN:")
    for tier in ["Critical","High","Medium","Low"]:
        count = (df["fraud_risk_tier"] == tier).sum()
        pct   = count / len(df) * 100
        print(f"    {tier:<10} : {count:,} transactions ({pct:.1f}%)")
    print("-"*68)
    print("  ALERTS BY CHANNEL:")
    channel_alerts = (
        df[df["requires_review"]]
        .groupby("channel")["transaction_id"].count()
        .sort_values(ascending=False)
    )
    for channel, count in channel_alerts.items():
        print(f"    {channel:<20} : {count:,} alerts")
    print("-"*68)
    print("  VALUE AT RISK BY REGION:")
    region_risk = (
        df[df["requires_review"]]
        .groupby("region")["amount_ghs"].sum()
        .sort_values(ascending=False)
    )
    for region, val in region_risk.items():
        print(f"    {region:<20} : GHS {val:,.2f}")
    print("="*68 + "\n")


def run_pipeline():
    logger.info("=" * 62)
    logger.info("  BANK FRAUD DETECTION PIPELINE — STARTED")
    logger.info("=" * 62)
    start = datetime.now()
    df = extract()
    df = transform(df)
    load(df)
    print_summary(df)
    duration = (datetime.now() - start).total_seconds()
    logger.info(f"PIPELINE COMPLETED in {duration:.2f} seconds")


if __name__ == "__main__":
    run_pipeline()