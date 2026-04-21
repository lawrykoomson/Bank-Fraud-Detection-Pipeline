"""
Real-Time Bank Transaction Fraud Stream Simulator
==================================================
Simulates Apache Kafka-style real-time fraud detection
for Absa Bank Ghana transactions.

Architecture:
    Producer          → generates live bank transaction events
    FraudDetector     → scores and flags suspicious transactions
    ComplianceLogger  → logs all flagged transactions
    MetricsConsumer   → aggregates real-time fraud KPIs

Author: Lawrence Koomson
GitHub: github.com/lawrykoomson
"""

import queue
import threading
import time
import random
import json
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("kafka_fraud.log"),
        logging.StreamHandler()
    ]
)

TOPIC_NAME         = "bank.transactions.live"
PARTITION_COUNT    = 3
PRODUCER_RATE_HZ   = 8
SIMULATION_SECONDS = 60

CHANNELS  = ["ATM","Online Banking","Mobile App","Branch Teller","POS Terminal"]
TXN_TYPES = ["Transfer","Withdrawal","Purchase","Bill Payment","Loan Repayment","Deposit"]
REGIONS   = ["Greater Accra","Ashanti","Western","Eastern","Northern"]

REPORTS_PATH = Path("data/reports/")
REPORTS_PATH.mkdir(parents=True, exist_ok=True)


class FraudTopic:
    def __init__(self, name, partitions=3):
        self.name       = name
        self.partitions = [queue.Queue() for _ in range(partitions)]
        self.counter    = 0
        self.lock       = threading.Lock()

    def produce(self, msg):
        with self.lock:
            pid = self.counter % len(self.partitions)
            self.partitions[pid].put(msg)
            self.counter += 1

    def consume(self, pid, timeout=0.1):
        try:
            return self.partitions[pid].get(timeout=timeout)
        except queue.Empty:
            return None


class BankTransactionProducer(threading.Thread):
    def __init__(self, topic, rate_hz, duration_secs):
        super().__init__(name="BankProducer", daemon=True)
        self.topic    = topic
        self.rate_hz  = rate_hz
        self.duration = duration_secs
        self.produced = 0
        self.running  = True
        self.logger   = logging.getLogger("BankProducer")
        self._counter = 1

    def generate_transaction(self):
        amount = random.choices(
            [random.uniform(1, 200),
             random.uniform(200, 2000),
             random.uniform(2000, 50000)],
            weights=[70, 25, 5]
        )[0]
        hour = random.choices(
            list(range(0, 6)) + list(range(6, 24)),
            weights=[1]*6 + [4]*18
        )[0]

        signals = []
        score   = 0
        if amount >= 5000:
            signals.append("HIGH_AMOUNT")
            score += 20
        if hour < 6:
            signals.append("UNUSUAL_HOUR")
            score += 15
        if random.random() < 0.20:
            signals.append("FOREIGN_IP")
            score += 15
        if random.random() < 0.15:
            signals.append("RAPID_SUCCESSION")
            score += 20
        if random.random() < 0.15:
            signals.append("AMOUNT_OUTLIER")
            score += 20

        if score >= 70:   tier = "Critical"
        elif score >= 46: tier = "High"
        elif score >= 21: tier = "Medium"
        else:             tier = "Low"

        return {
            "event_id":       f"EVT-{str(self._counter).zfill(8)}",
            "transaction_id": f"TXN-LIVE-{str(self._counter).zfill(9)}",
            "timestamp":      datetime.now().isoformat(),
            "channel":        random.choices(CHANNELS, weights=[20,30,30,10,10])[0],
            "region":         random.choices(REGIONS,  weights=[35,25,18,12,10])[0],
            "transaction_type": random.choice(TXN_TYPES),
            "amount_ghs":     round(amount, 2),
            "hour":           hour,
            "ip_country":     random.choices(["GH","NG","US","CN"], weights=[92,3,3,2])[0],
            "fraud_score":    min(score, 100),
            "fraud_tier":     tier,
            "signals":        signals,
            "requires_review": score >= 46,
            "is_fraud":       1 if random.random() < 0.03 else 0,
        }

    def run(self):
        self.logger.info(f"Producer started → '{self.topic.name}' at {self.rate_hz} txn/sec")
        end_time   = time.time() + self.duration
        sleep_time = 1.0 / self.rate_hz
        while self.running and time.time() < end_time:
            self.topic.produce(self.generate_transaction())
            self.produced  += 1
            self._counter  += 1
            time.sleep(sleep_time)
        self.running = False
        self.logger.info(f"Producer finished — published {self.produced:,} transactions")


class FraudDetectorConsumer(threading.Thread):
    def __init__(self, topic):
        super().__init__(name="FraudDetector", daemon=True)
        self.topic   = topic
        self.running = True
        self.alerts  = []
        self.logger  = logging.getLogger("FraudDetector")

    def run(self):
        self.logger.info("Consumer started — fraud detection on partition 0")
        while self.running:
            msg = self.topic.consume(0)
            if msg is None:
                continue
            if msg["requires_review"]:
                self.alerts.append(msg)
                level = "CRITICAL" if msg["fraud_tier"] == "Critical" else "WARNING"
                self.logger.warning(
                    f"{level} | {msg['transaction_id']} | "
                    f"Score: {msg['fraud_score']} | "
                    f"GHS {msg['amount_ghs']:,.2f} | "
                    f"Signals: {', '.join(msg['signals'])}"
                )


class MetricsConsumer(threading.Thread):
    def __init__(self, topic):
        super().__init__(name="MetricsConsumer", daemon=True)
        self.topic   = topic
        self.running = True
        self.logger  = logging.getLogger("MetricsConsumer")
        self.m = {
            "total": 0, "flagged": 0, "critical": 0,
            "fraud": 0, "total_volume": 0.0, "flagged_volume": 0.0,
            "by_channel": {}, "by_region": {}
        }

    def run(self):
        self.logger.info("Consumer started — aggregating metrics on partition 1")
        while self.running:
            msg = self.topic.consume(1)
            if msg is None:
                continue
            m = self.m
            m["total"]        += 1
            m["total_volume"] += msg["amount_ghs"]
            m["fraud"]        += msg["is_fraud"]
            if msg["requires_review"]:
                m["flagged"]        += 1
                m["flagged_volume"] += msg["amount_ghs"]
            if msg["fraud_tier"] == "Critical":
                m["critical"] += 1
            m["by_channel"][msg["channel"]] = \
                m["by_channel"].get(msg["channel"], 0) + 1
            m["by_region"][msg["region"]] = \
                m["by_region"].get(msg["region"], 0) + msg["amount_ghs"]

    def snapshot(self):
        m = self.m
        return {
            "total":          m["total"],
            "flagged":        m["flagged"],
            "critical":       m["critical"],
            "fraud_count":    m["fraud"],
            "total_volume":   round(m["total_volume"], 2),
            "flagged_volume": round(m["flagged_volume"], 2),
            "top_channel":    max(m["by_channel"], key=m["by_channel"].get, default="N/A"),
            "top_region":     max(m["by_region"],  key=m["by_region"].get,  default="N/A"),
        }


class ComplianceLoggerConsumer(threading.Thread):
    def __init__(self, topic):
        super().__init__(name="ComplianceLogger", daemon=True)
        self.topic    = topic
        self.running  = True
        self.consumed = 0
        self.logger   = logging.getLogger("ComplianceLogger")
        self.log_file = REPORTS_PATH / "fraud_events_live.jsonl"

    def run(self):
        self.logger.info(f"Consumer started — logging to {self.log_file}")
        with open(self.log_file, "w") as f:
            while self.running:
                msg = self.topic.consume(2)
                if msg is None:
                    continue
                self.consumed += 1
                f.write(json.dumps(msg) + "\n")
                f.flush()


def print_live_metrics(producer, metrics, fraud, compliance, interval=10):
    start = time.time()
    while producer.running:
        time.sleep(interval)
        elapsed = int(time.time() - start)
        snap    = metrics.snapshot()
        print("\n" + "="*65)
        print(f"  FRAUD STREAM — LIVE METRICS  [{elapsed}s elapsed]")
        print("="*65)
        print(f"  Transactions Produced : {producer.produced:,}")
        print(f"  Throughput            : {producer.produced/max(elapsed,1):.1f} txn/sec")
        print(f"  Total Scored          : {snap['total']:,}")
        print(f"  Flagged for Review    : {snap['flagged']:,}")
        print(f"  Critical Risk         : {snap['critical']:,}")
        print(f"  Actual Fraud Detected : {snap['fraud_count']:,}")
        print(f"  Total Volume          : GHS {snap['total_volume']:,.2f}")
        print(f"  Flagged Value         : GHS {snap['flagged_volume']:,.2f}")
        print(f"  Top Channel           : {snap['top_channel']}")
        print(f"  Top Region            : {snap['top_region']}")
        print(f"  Compliance Alerts     : {len(fraud.alerts):,}")
        print(f"  Events Logged         : {compliance.consumed:,}")
        print("="*65)


def run_kafka_fraud_simulator():
    print("\n" + "="*65)
    print("  ABSA BANK GHANA — FRAUD KAFKA STREAM SIMULATOR")
    print("  Architecture: Producer → Topic → 3 Consumer Groups")
    print("="*65)
    print(f"  Topic          : {TOPIC_NAME}")
    print(f"  Partitions     : {PARTITION_COUNT}")
    print(f"  Producer Rate  : {PRODUCER_RATE_HZ} txn/sec")
    print(f"  Duration       : {SIMULATION_SECONDS} seconds")
    print(f"  Expected       : ~{PRODUCER_RATE_HZ * SIMULATION_SECONDS:,} transactions")
    print("="*65 + "\n")

    topic      = FraudTopic(TOPIC_NAME, PARTITION_COUNT)
    producer   = BankTransactionProducer(topic, PRODUCER_RATE_HZ, SIMULATION_SECONDS)
    fraud      = FraudDetectorConsumer(topic)
    metrics    = MetricsConsumer(topic)
    compliance = ComplianceLoggerConsumer(topic)

    for t in [producer, fraud, metrics, compliance]:
        t.start()

    m_thread = threading.Thread(
        target=print_live_metrics,
        args=(producer, metrics, fraud, compliance, 10),
        daemon=True
    )
    m_thread.start()
    producer.join()
    time.sleep(3)
    for t in [fraud, metrics, compliance]:
        t.running = False

    final = metrics.snapshot()
    print("\n" + "="*65)
    print("  FRAUD KAFKA SIMULATION — FINAL SUMMARY")
    print("="*65)
    print(f"  Total Transactions     : {producer.produced:,}")
    print(f"  Flagged for Review     : {final['flagged']:,}")
    print(f"  Critical Risk          : {final['critical']:,}")
    print(f"  Actual Fraud Detected  : {final['fraud_count']:,}")
    print(f"  Total Volume Streamed  : GHS {final['total_volume']:,.2f}")
    print(f"  Flagged Value          : GHS {final['flagged_volume']:,.2f}")
    print(f"  Compliance Alerts      : {len(fraud.alerts):,}")
    print(f"  Top Channel            : {final['top_channel']}")
    print(f"  Top Region             : {final['top_region']}")
    print(f"  Events Logged          : {compliance.consumed:,}")
    print("="*65 + "\n")

    if fraud.alerts:
        import csv
        alerts_path = REPORTS_PATH / "fraud_compliance_alerts.csv"
        with open(alerts_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fraud.alerts[0].keys())
            writer.writeheader()
            writer.writerows(fraud.alerts)
        print(f"  Compliance alerts saved: {alerts_path}")


if __name__ == "__main__":
    run_kafka_fraud_simulator()