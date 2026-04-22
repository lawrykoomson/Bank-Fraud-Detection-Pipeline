# 🏦 Bank Transaction Fraud Detection Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat-square&logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-336791?style=flat-square&logo=postgresql)
![dbt](https://img.shields.io/badge/dbt-1.11-FF694B?style=flat-square&logo=dbt)
![PowerBI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=flat-square&logo=powerbi)
![Tests](https://img.shields.io/badge/Tests-25%2F25%20Passing-brightgreen?style=flat-square)
![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen?style=flat-square)

A production-grade fraud detection pipeline that processes **20,000 Absa Bank Ghana transactions**, engineers 6 fraud-detection signals per transaction, scores every transaction for risk, and loads flagged cases into a live **PostgreSQL** warehouse — with a full **dbt analytical layer**, **Power BI compliance dashboard**, **Airflow DAG**, and **Kafka stream simulator**.

Built to mirror real fraud detection workflows used at **Absa Bank Ghana**.

---

## 🏗️ System Architecture

```
[Bank Transaction Data Source]
           │
           ▼
     ┌───────────┐
     │  EXTRACT  │  ← Generates 20,000 synthetic Absa Bank Ghana transactions
     └───────────┘
           │
           ▼
     ┌───────────┐
     │ TRANSFORM │  ← Engineers 6 fraud signals + scores every transaction
     └───────────┘
           │
           ▼
     ┌───────────┐
     │   LOAD    │  ← PostgreSQL warehouse (fraud_detection schema)
     └───────────┘
           │
           ▼
     ┌───────────┐
     │    dbt    │  ← Analytical layer: 1 staging view + 3 mart tables
     └───────────┘
           │
           ▼
     ┌───────────┐
     │  Power BI │  ← 4-page compliance dashboard connected to PostgreSQL
     └───────────┘
           │
           ▼
     ┌───────────┐
     │   Kafka   │  ← Real-time fraud stream: Producer + 3 Consumer Groups
     └───────────┘
```

---

## ✅ What The Pipeline Does

### Extract
- Generates 20,000 realistic Absa Bank Ghana transactions
- 5 channels: ATM, Online Banking, Mobile App, Branch Teller, POS Terminal
- 5 Ghana regions with realistic distribution
- 3% ground-truth fraud injected for model validation

### Transform — Fraud Signal Engineering
Each transaction is scored using 6 binary fraud signals:

| Signal | Weight | Trigger |
|---|---|---|
| High Amount | 20% | Transaction >= GHS 5,000 |
| Rapid Succession | 20% | Transaction within 5 mins of previous |
| Amount Outlier | 20% | Z-score > 3.0 vs account history |
| Unusual Hour | 15% | Between 00:00 and 05:00 |
| Foreign IP | 15% | IP origin outside Ghana |
| High Frequency | 10% | More than 10 transactions in one day |

### Risk Tiers
| Score | Tier | Action |
|---|---|---|
| 71–100 | Critical | Immediate block + compliance escalation |
| 46–70 | High | Flag for senior reviewer |
| 21–45 | Medium | Queue for next-day review |
| 0–20 | Low | Monitor — no action required |

### Load
- Batch upserts into PostgreSQL (fraud_detection schema)
- Creates separate alerts table for flagged transactions
- Auto-falls back to CSV if database unavailable

---

## 🔁 dbt Analytical Layer

4 models built on top of PostgreSQL:

| Model | Type | Description |
|---|---|---|
| stg_fraud_transactions | View | Cleaned transactions + risk labels |
| mart_fraud_by_channel | Table | Fraud metrics per banking channel |
| mart_fraud_by_region | Table | Fraud metrics per Ghana region |
| mart_high_risk_transactions | Table | 106 flagged High/Critical transactions |

```bash
cd dbt
dbt run --profiles-dir .    # Run all 4 models
dbt test --profiles-dir .   # Run 4 data quality tests
```

---

## 📊 Power BI Dashboard — 4 Pages

Connected live to PostgreSQL via dbt mart tables:

| Page | Key Metrics |
|---|---|
| Executive Summary | 20K transactions, 106 flagged, GHS 2.97M flagged value |
| Channel Analysis | Mobile App leads flags, flag rate and risk score by channel |
| Regional Analysis | Greater Accra 47.08% flagged value share |
| High Risk Transactions | 106 HIGH tier, full alert details per transaction |

---

## 🌊 Kafka Stream Simulator

Real-time fraud detection streaming:

```bash
py -3.11 kafka_fraud_simulator.py
```

```
Topic          : bank.transactions.live
Partitions     : 3
Producer Rate  : 8 transactions/sec
Duration       : 60 seconds

Producer          → generates live bank transaction events
FraudDetector     → scores and flags suspicious transactions (partition 0)
MetricsConsumer   → aggregates real-time fraud KPIs (partition 1)
ComplianceLogger  → logs all events to JSONL file (partition 2)

Final Results:
  Total Transactions     : 477
  Flagged for Review     : 2
  Actual Fraud Detected  : 8
  Total Volume Streamed  : GHS 197,540.95
  Top Channel            : Mobile App
  Top Region             : Greater Accra
```

---

## 🧪 Unit Tests — 25/25 Passing

```bash
py -3.11 -m pytest test_fraud_pipeline.py -v
# 25 passed in 22.98s
```

| Test Class | Tests | Coverage |
|---|---|---|
| TestExtract | 9 | Row count, columns, fraud rate, valid values |
| TestTransform | 11 | Score ranges, tiers, signals, time features |
| TestIntegration | 5 | End-to-end, alerts, thresholds, flags |

---

## 📋 Airflow DAG

Scheduled pipeline at `dags/fraud_pipeline_dag.py`:
- Runs **every day at 02:00 AM UTC** (overnight batch processing)
- 5 tasks: extract, score, load, dbt refresh, notify compliance
- XCom passes flagged count and value between tasks
- Email alerts on failure with 2 retries

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
    Critical   : 0    transactions (0.0%)
    High       : 106  transactions (0.5%)
    Medium     : 830  transactions (4.2%)
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
====================================================================
```

---

## 🚀 How To Run

```bash
# 1. Clone the repo
git clone https://github.com/lawrykoomson/Bank-Fraud-Detection-Pipeline.git
cd Bank-Fraud-Detection-Pipeline

# 2. Install dependencies
py -3.11 -m pip install -r requirements.txt

# 3. Create PostgreSQL database
psql -U postgres -c "CREATE DATABASE bank_analytics;"

# 4. Configure environment
copy .env.example .env
# Edit .env with your PostgreSQL credentials

# 5. Run the pipeline
py -3.11 fraud_pipeline.py

# 6. Run unit tests
py -3.11 -m pytest test_fraud_pipeline.py -v

# 7. Run dbt models
cd dbt
set DBT_POSTGRES_PASSWORD=your_password
dbt run --profiles-dir .
dbt test --profiles-dir .

# 8. Run Kafka stream simulator
cd ..
py -3.11 kafka_fraud_simulator.py
```

---

## 📦 Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.11 | Core pipeline language |
| Pandas | Data extraction and transformation |
| NumPy | Numerical operations and z-score calculation |
| psycopg2 | PostgreSQL database connector |
| dbt-postgres | Analytical transformation layer |
| Apache Airflow | Pipeline orchestration DAG |
| Power BI | Compliance fraud dashboard |
| pytest | Unit testing framework |
| python-dotenv | Environment variable management |

---

## 🔮 Roadmap

- [x] ETL pipeline with PostgreSQL live load
- [x] 25 unit tests — all passing
- [x] dbt analytical layer — 4 models, 4 tests passing
- [x] Apache Airflow DAG — daily scheduled runs
- [x] Power BI dashboard — 4 pages live
- [x] Kafka stream simulator — 3 consumer groups
- [ ] Docker containerisation
- [ ] ML-based fraud scoring model

---

## 👨‍💻 Author

**Lawrence Koomson**
BSc. Information Technology — Data Engineering | University of Cape Coast, Ghana
🔗 [LinkedIn](https://linkedin.com/in/lawrykoomson) | [GitHub](https://github.com/lawrykoomson)
