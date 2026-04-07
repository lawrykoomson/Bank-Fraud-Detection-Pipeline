# 🏦 Bank Transaction Fraud Detection Pipeline

![Python](https://img.shields.io/badge/Python-3.14-blue?style=flat-square&logo=python)
![Pandas](https://img.shields.io/badge/Pandas-3.0.2-150458?style=flat-square&logo=pandas)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=flat-square&logo=postgresql)
![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=flat-square)

A production-grade data engineering pipeline that processes bank transaction records, engineers multi-layer fraud detection features, scores each transaction's risk level, flags suspicious activity for compliance review, and loads results into PostgreSQL.

Built to reflect real fraud monitoring workflows used in Ghanaian banks like **Absa Bank Ghana**.

---

## 🏗️ Pipeline Architecture
```
[Core Banking System / Transaction Logs]
              │
              ▼
        ┌───────────┐
        │  EXTRACT  │  ← 20,000 bank transactions with injected fraud
        └───────────┘
              │
              ▼
        ┌───────────┐
        │ TRANSFORM │  ← Multi-layer fraud feature engineering
        └───────────┘
              │
              ▼
        ┌───────────┐
        │   LOAD    │  ← PostgreSQL: fact table + alerts table
        └───────────┘
              │
              ▼
        ┌───────────┐
        │  REPORT   │  ← Fraud summary by channel, region, signal
        └───────────┘
```

---

## 🔍 Fraud Detection Signals

| Signal | Description | Risk Weight |
|---|---|---|
| `is_high_amount` | Transaction ≥ GHS 5,000 | 20% |
| `is_rapid_txn` | < 5 mins since last transaction | 20% |
| `is_amount_outlier` | Z-score > 3 vs account history | 20% |
| `is_unusual_hour` | Between midnight and 5am | 15% |
| `is_foreign_ip` | IP origin outside Ghana | 15% |
| `is_high_frequency` | > 10 transactions in a day | 10% |

### Risk Tiers
| Score | Tier | Action |
|---|---|---|
| 0 – 20 | 🟢 Low | No action |
| 21 – 45 | 🟡 Medium | Monitor |
| 46 – 70 | 🟠 High | Flag for compliance review |
| 71 – 100 | 🔴 Critical | Immediate investigation |

---

## 📊 Sample Pipeline Output
```
====================================================================
   BANK FRAUD DETECTION PIPELINE — RUN SUMMARY
====================================================================
  Total Transactions Processed : 20,000
  Flagged for Review           : 106 (0.5%)
  Critical Risk Transactions   : 0
  Total Value Flagged          : GHS 2,967,551.72
  Actual Fraud (Ground Truth)  : 600 (3.0%)
--------------------------------------------------------------------
  FRAUD RISK TIER BREAKDOWN:
    Critical   : 0   transactions (0.0%)
    High       : 106 transactions (0.5%)
    Medium     : 830 transactions (4.2%)
    Low        : 19,064 transactions (95.3%)
--------------------------------------------------------------------
  ALERTS BY CHANNEL:
    Mobile App           : 38 alerts
    Online Banking       : 32 alerts
    ATM                  : 17 alerts
    POS Terminal         : 13 alerts
    Branch Teller        : 6 alerts
--------------------------------------------------------------------
  VALUE AT RISK BY REGION:
    Greater Accra        : GHS 1,397,135.53
    Ashanti              : GHS   664,052.81
    Western              : GHS   384,834.51
    Eastern              : GHS   302,556.48
    Northern             : GHS   218,972.39
--------------------------------------------------------------------
  TOP FRAUD SIGNALS TRIGGERED:
    Unusual Hour         : 4,901 transactions
    Foreign IP           : 1,693 transactions
    High Amount          : 1,029 transactions
    Amount Outlier       : 360  transactions
    Rapid Succession     : 3    transactions
    High Frequency       : 0    transactions
====================================================================
```

---

## 🗄️ Database Schema
```sql
-- Main fact table
fraud_detection.fact_transactions (
    transaction_id, timestamp, txn_date, txn_hour,
    sender_account, receiver_account, amount_ghs,
    transaction_type, channel, region, status,
    is_unusual_hour, is_high_amount, amount_z_score,
    is_rapid_txn, is_foreign_ip, is_high_frequency,
    fraud_risk_score, fraud_risk_tier,
    requires_review, alert_reason,
    is_fraud_actual, processed_at
)

-- Compliance alerts table
fraud_detection.alerts (
    alert_id, transaction_id, risk_tier,
    fraud_score, alert_reason, amount_ghs,
    channel, region, created_at
)
```

---

## 🚀 How To Run

### 1. Clone the repo
```bash
git clone https://github.com/lawrykoomson/Bank-Fraud-Detection-Pipeline.git
cd Bank-Fraud-Detection-Pipeline
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure environment
```bash
copy .env.example .env
# Edit .env with your PostgreSQL credentials (optional)
```

### 4. Run the pipeline
```bash
python fraud_pipeline.py
```
> No database? Results auto-save to `data/processed/` as CSV.

---

## 📦 Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.14 | Core pipeline language |
| Pandas | Data transformation and feature engineering |
| NumPy | Statistical calculations and synthetic data |
| psycopg2 | PostgreSQL database connector |
| python-dotenv | Environment variable management |

---

## 🔮 Future Improvements
- [ ] Replace rule-based scoring with XGBoost trained on labelled fraud data
- [ ] Real-time streaming pipeline using Apache Kafka
- [ ] Apache Airflow DAG for hourly scheduled runs
- [ ] Power BI live fraud monitoring dashboard
- [ ] Model performance tracking (precision, recall, F1)
- [ ] Email/SMS compliance alert integration

---

## 👨‍💻 Author

**Lawrence Koomson**
BSc. Information Technology — Data Engineering | University of Cape Coast, Ghana
🔗 [LinkedIn](https://linkedin.com/in/lawrykoomson) | [GitHub](https://github.com/lawrykoomson)