"""
Unit Tests — Bank Fraud Detection Pipeline
============================================
Run with: pytest test_fraud_pipeline.py -v

Author: Lawrence Koomson
"""

import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from fraud_pipeline import extract, transform


class TestExtract:

    def test_returns_dataframe(self):
        df = extract()
        assert isinstance(df, pd.DataFrame)

    def test_correct_row_count(self):
        df = extract()
        assert len(df) == 20000

    def test_required_columns_present(self):
        df = extract()
        required = [
            "transaction_id","timestamp","sender_account",
            "receiver_account","amount_ghs","transaction_type",
            "channel","region","currency","status",
            "device_id","ip_country","is_fraud_actual"
        ]
        for col in required:
            assert col in df.columns, f"Missing column: {col}"

    def test_transaction_ids_unique(self):
        df = extract()
        assert df["transaction_id"].nunique() == len(df)

    def test_amounts_positive(self):
        df = extract()
        assert (df["amount_ghs"] > 0).all()

    def test_fraud_column_binary(self):
        df = extract()
        assert set(df["is_fraud_actual"].unique()).issubset({0, 1})

    def test_fraud_rate_realistic(self):
        df = extract()
        fraud_rate = df["is_fraud_actual"].mean()
        assert 0.01 < fraud_rate < 0.10

    def test_channels_valid(self):
        df = extract()
        valid = {"ATM","Online Banking","Mobile App","Branch Teller","POS Terminal"}
        assert set(df["channel"].unique()).issubset(valid)

    def test_regions_valid(self):
        df = extract()
        valid = {"Greater Accra","Ashanti","Western","Eastern","Northern"}
        assert set(df["region"].unique()).issubset(valid)


class TestTransform:

    @pytest.fixture
    def transformed(self):
        df = extract()
        return transform(df)

    def test_fraud_risk_score_range(self, transformed):
        assert transformed["fraud_risk_score"].between(0, 100).all()

    def test_risk_tiers_valid(self, transformed):
        valid = {"Low","Medium","High","Critical"}
        assert set(transformed["fraud_risk_tier"].unique()).issubset(valid)

    def test_requires_review_boolean(self, transformed):
        assert transformed["requires_review"].dtype == bool

    def test_high_risk_flagged_correctly(self, transformed):
        high_risk = transformed[transformed["requires_review"]]
        assert (high_risk["fraud_risk_score"] >= 46).all()

    def test_alert_reason_populated(self, transformed):
        flagged = transformed[transformed["requires_review"]]
        assert (flagged["alert_reason"] != "No flags").all()

    def test_time_features_created(self, transformed):
        for col in ["txn_hour","txn_date","txn_day_of_week","is_weekend"]:
            assert col in transformed.columns

    def test_fraud_signals_created(self, transformed):
        for col in ["is_high_amount","is_rapid_txn","is_amount_outlier",
                    "is_unusual_hour","is_foreign_ip","is_high_frequency"]:
            assert col in transformed.columns

    def test_no_null_risk_scores(self, transformed):
        assert transformed["fraud_risk_score"].isna().sum() == 0

    def test_amount_z_score_calculated(self, transformed):
        assert "amount_z_score" in transformed.columns
        assert transformed["amount_z_score"].isna().sum() == 0

    def test_row_count_preserved(self, transformed):
        df = extract()
        assert len(transformed) == len(df)

    def test_processed_at_exists(self, transformed):
        assert "processed_at" in transformed.columns


class TestIntegration:

    def test_full_pipeline_runs(self):
        df     = extract()
        result = transform(df)
        assert len(result) == len(df)

    def test_flagged_transactions_have_alerts(self):
        df     = extract()
        result = transform(df)
        flagged = result[result["requires_review"]]
        assert len(flagged) > 0

    def test_no_duplicate_transaction_ids(self):
        df     = extract()
        result = transform(df)
        assert result["transaction_id"].duplicated().sum() == 0

    def test_high_value_threshold_applied(self):
        df     = extract()
        result = transform(df)
        high_val = result[result["is_high_amount"]]
        assert (high_val["amount_ghs"] >= 5000).all()

    def test_foreign_ip_flag_correct(self):
        df     = extract()
        result = transform(df)
        foreign = result[result["is_foreign_ip"]]
        assert (foreign["ip_country"] != "GH").all()