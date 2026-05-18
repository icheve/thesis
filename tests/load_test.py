"""
Нагрузочное тестирование NRT Payment Pipeline.

Сценарии:
  baseline  — постоянная нагрузка (TC-C01)
  ramp      — нарастающая нагрузка (TC-C02)

Запуск:
  python tests/load_test.py --scenario baseline --rps 200 --duration 300 --output results/load_report.json
  python tests/load_test.py --scenario ramp --rps-start 200 --rps-peak 2000 \
      --ramp-steps 3 --step-duration 120 --output results/load_report.json
  python tests/load_test.py --report --prometheus http://localhost:9090 \
      --output results/load_report.json
"""

import argparse
import json
import math
import os
import random
import statistics
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

try:
    from confluent_kafka import Producer
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False
    print("[WARN] confluent-kafka не установлен; события отправляться не будут.")

# ──────────────────────────────────────────────────────────────
#  Конфигурация
# ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "payments")
RAW_TOPIC = "payments.raw"

CURRENCIES = ["USD", "EUR", "GBP", "RUB", "CNY"]
SOURCES = ["MOBILE_APP", "ACQUIRING", "INTERNET_BANKING", "BATCH_TRANSFER"]
STATUSES = ["INITIATED", "PROCESSING", "COMPLETED", "FAILED", "REVERSED"]
EVENT_TYPES = ["PAYMENT_CREATED", "STATUS_CHANGED", "PAYMENT_COMPLETED",
               "PAYMENT_FAILED", "PAYMENT_REVERSED"]
MERCHANTS = [f"mrc-{i:03d}" for i in range(1, 21)]


# ──────────────────────────────────────────────────────────────
#  Модель результатов теста
# ──────────────────────────────────────────────────────────────

@dataclass
class StepResult:
    rps_target: float
    rps_actual: float
    duration_s: float
    events_sent: int
    errors: int
    latencies_ms: list[float] = field(default_factory=list)

    @property
    def p50(self) -> float:
        return statistics.median(self.latencies_ms) if self.latencies_ms else 0.0

    @property
    def p99(self) -> float:
        if not self.latencies_ms:
            return 0.0
        sorted_lat = sorted(self.latencies_ms)
        idx = max(0, math.ceil(0.99 * len(sorted_lat)) - 1)
        return sorted_lat[idx]

    @property
    def error_rate_pct(self) -> float:
        total = self.events_sent + self.errors
        return (self.errors / total * 100) if total else 0.0


@dataclass
class LoadTestReport:
    scenario: str
    start_time: str
    end_time: str
    total_events_sent: int
    total_errors: int
    steps: list[StepResult] = field(default_factory=list)
    prometheus_metrics: dict = field(default_factory=dict)
    sla_passed: Optional[bool] = None
    sla_details: dict = field(default_factory=dict)


# ──────────────────────────────────────────────────────────────
#  Генерация событий
# ──────────────────────────────────────────────────────────────

def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def generate_event() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "payment_id": f"pay-load-{uuid.uuid4().hex[:12]}",
        "status": random.choice(STATUSES),
        "amount": str(round(random.lognormvariate(7.5, 1.2), 2)),
        "currency": random.choice(CURRENCIES),
        "source_system": random.choice(SOURCES),
        "event_type": random.choice(EVENT_TYPES),
        "merchant_id": random.choice(MERCHANTS),
        "payer_id": f"usr-{random.randint(1, 100000):06d}",
        "event_ts": _now_ms(),
        "ingestion_ts": _now_ms(),
        "version": 1,
    }


# ──────────────────────────────────────────────────────────────
#  Отправка в Kafka
# ──────────────────────────────────────────────────────────────

class KafkaSender:
    def __init__(self):
        if not HAS_KAFKA:
            self._producer = None
            return
        self._producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "enable.idempotence": "true",
            "acks": "all",
            "linger.ms": 5,
            "batch.num.messages": 1000,
            "compression.type": "lz4",
        })
        self._lock = threading.Lock()
        self._error_count = 0

    def send(self, event: dict) -> float:
        """Отправить событие; вернуть задержку в ms."""
        if not self._producer:
            return 0.0
        payload = json.dumps(event).encode()
        start = time.perf_counter()
        try:
            self._producer.produce(
                RAW_TOPIC,
                value=payload,
                key=event["payment_id"].encode(),
                on_delivery=self._on_delivery,
            )
            self._producer.poll(0)
        except BufferError:
            self._producer.poll(0.1)
            self._producer.produce(RAW_TOPIC, value=payload,
                                   key=event["payment_id"].encode())
        return (time.perf_counter() - start) * 1000

    def _on_delivery(self, err, _msg):
        if err:
            with self._lock:
                self._error_count += 1

    def flush(self):
        if self._producer:
            self._producer.flush(10)

    @property
    def error_count(self) -> int:
        return self._error_count


# ──────────────────────────────────────────────────────────────
#  Прогон одного шага нагрузки
# ──────────────────────────────────────────────────────────────

def run_step(sender: KafkaSender, rps: float, duration_s: float,
             workers: int = 8) -> StepResult:
    """
    Отправлять события с заданным RPS в течение duration_s секунд.
    workers — число параллельных потоков-отправителей.
    """
    interval = 1.0 / rps if rps > 0 else 1.0
    events_sent = 0
    latencies: list[float] = []
    lat_lock = threading.Lock()
    stop_event = threading.Event()

    def worker():
        nonlocal events_sent
        while not stop_event.is_set():
            t0 = time.perf_counter()
            evt = generate_event()
            lat = sender.send(evt)
            with lat_lock:
                latencies.append(lat)
                events_sent += 1
            elapsed = time.perf_counter() - t0
            sleep = (interval * workers) - elapsed
            if sleep > 0:
                time.sleep(sleep)

    start = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker) for _ in range(workers)]
        time.sleep(duration_s)
        stop_event.set()
        for f in as_completed(futures, timeout=5):
            try:
                f.result()
            except Exception:
                pass

    sender.flush()
    elapsed = time.time() - start
    actual_rps = events_sent / elapsed if elapsed > 0 else 0.0

    return StepResult(
        rps_target=rps,
        rps_actual=round(actual_rps, 1),
        duration_s=round(elapsed, 1),
        events_sent=events_sent,
        errors=sender.error_count,
        latencies_ms=latencies,
    )


# ──────────────────────────────────────────────────────────────
#  Prometheus-запросы
# ──────────────────────────────────────────────────────────────

def prom_query(url: str, promql: str) -> Optional[float]:
    """Выполнить instant-запрос к Prometheus; вернуть скалярное значение или None."""
    try:
        resp = requests.get(
            f"{url}/api/v1/query",
            params={"query": promql},
            timeout=5,
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("data", {}).get("result", [])
        if results:
            return float(results[0]["value"][1])
    except Exception as e:
        print(f"    [PROM] Не удалось выполнить запрос '{promql}': {e}")
    return None


def collect_prometheus_metrics(prom_url: str) -> dict:
    metrics = {}
    queries = {
        # e2e latency — gauge выставляется в ScdMergerOperator (pipeline_e2e_latency_seconds)
        # Flink Prometheus reporter добавляет префикс flink_taskmanager_job_task_operator_
        "e2e_latency_p99": (
            "quantile(0.99, flink_taskmanager_job_task_operator_pipeline_e2e_latency_seconds)"
        ),
        "e2e_latency_p50": (
            "quantile(0.50, flink_taskmanager_job_task_operator_pipeline_e2e_latency_seconds)"
        ),
        "consumer_lag_max": "max(kafka_consumergroup_lag{topic='payments.raw'})",
        "flink_checkpoint_duration_ms": (
            "flink_jobmanager_job_lastCheckpointDuration"
        ),
        # новые версии (записи в ClickHouse) в секунду
        "events_per_sec": (
            "sum(rate(flink_taskmanager_job_task_operator_new_versions_total[1m]))"
        ),
        # DLQ rate: validation_errors / total_processed (оба — накопленные счётчики)
        "dlq_rate_pct": (
            "100 * sum(flink_taskmanager_job_task_operator_validation_errors_total)"
            " / sum(flink_taskmanager_job_task_operator_dq_events_total)"
        ),
        "ch_insert_errors": (
            "sum(flink_taskmanager_job_task_operator_clickhouse_insert_errors_total)"
            " or vector(0)"
        ),
    }
    for name, promql in queries.items():
        val = prom_query(prom_url, promql)
        metrics[name] = val
        label = f"{val:.4f}" if val is not None else "N/A"
        print(f"    {name:35s} = {label}")
    return metrics


# ──────────────────────────────────────────────────────────────
#  Сценарии
# ──────────────────────────────────────────────────────────────

def scenario_baseline(args) -> LoadTestReport:
    """TC-C01: постоянная нагрузка."""
    print(f"\n[SCENARIO] baseline  RPS={args.rps}  duration={args.duration}s")
    sender = KafkaSender()
    report = LoadTestReport(
        scenario="baseline",
        start_time=datetime.now(timezone.utc).isoformat(),
        end_time="",
        total_events_sent=0,
        total_errors=0,
    )

    step = run_step(sender, rps=args.rps, duration_s=args.duration)
    report.steps.append(step)
    report.end_time = datetime.now(timezone.utc).isoformat()
    report.total_events_sent = step.events_sent
    report.total_errors = step.errors

    print(f"\n  Отправлено:     {step.events_sent}")
    print(f"  Ошибки:         {step.errors}  ({step.error_rate_pct:.2f}%)")
    print(f"  RPS фактический:{step.rps_actual}")
    print(f"  Produce p50:    {step.p50:.1f}ms")
    print(f"  Produce p99:    {step.p99:.1f}ms")

    print("\n  Сбор метрик из Prometheus (ожидание 30s для накопления)...")
    time.sleep(30)
    report.prometheus_metrics = collect_prometheus_metrics(args.prometheus)

    _evaluate_sla_baseline(report, args)
    return report


def scenario_ramp(args) -> LoadTestReport:
    """TC-C02: нарастающая нагрузка."""
    rps_levels = _ramp_levels(args.rps_start, args.rps_peak, args.ramp_steps)
    print(f"\n[SCENARIO] ramp  levels={rps_levels}  step_duration={args.step_duration}s")

    sender = KafkaSender()
    report = LoadTestReport(
        scenario="ramp",
        start_time=datetime.now(timezone.utc).isoformat(),
        end_time="",
        total_events_sent=0,
        total_errors=0,
    )

    for i, rps in enumerate(rps_levels, 1):
        print(f"\n  [Шаг {i}/{len(rps_levels)}] RPS={rps}")
        step = run_step(sender, rps=rps, duration_s=args.step_duration)
        report.steps.append(step)
        report.total_events_sent += step.events_sent
        report.total_errors += step.errors
        print(f"    Отправлено: {step.events_sent}, "
              f"RPS факт.: {step.rps_actual}, "
              f"Ошибки: {step.errors} ({step.error_rate_pct:.2f}%)")

    report.end_time = datetime.now(timezone.utc).isoformat()

    print("\n  Ожидание стабилизации (60s)...")
    time.sleep(60)
    print("  Сбор метрик из Prometheus:")
    report.prometheus_metrics = collect_prometheus_metrics(args.prometheus)

    _evaluate_sla_ramp(report, args)
    return report


def _ramp_levels(start: float, peak: float, steps: int) -> list[float]:
    if steps <= 1:
        return [start, peak]
    result = []
    for i in range(steps):
        val = start + (peak - start) * i / (steps - 1)
        result.append(round(val))
    return result


def _evaluate_sla_baseline(report: LoadTestReport, args):
    m = report.prometheus_metrics
    lat_p99 = m.get("e2e_latency_p99")  # в секундах
    step = report.steps[0]

    checks = {}
    if lat_p99 is not None:
        checks["e2e_p99_le_30s"] = lat_p99 <= 30.0
        checks["e2e_p99_value_s"] = round(lat_p99, 2)
    checks["error_rate_lt_1pct"] = step.error_rate_pct < 1.0
    checks["error_rate_value_pct"] = round(step.error_rate_pct, 3)

    report.sla_details = checks
    report.sla_passed = all(v for k, v in checks.items()
                            if isinstance(v, bool))
    status = "PASSED ✓" if report.sla_passed else "FAILED ✗"
    print(f"\n  SLA результат: {status}")
    for k, v in checks.items():
        print(f"    {k}: {v}")


def _evaluate_sla_ramp(report: LoadTestReport, args):
    m = report.prometheus_metrics
    lag = m.get("consumer_lag_max")

    checks = {}
    # После снижения нагрузки lag должен быть < 1000
    if lag is not None:
        checks["consumer_lag_recovering"] = lag < 1000
        checks["consumer_lag_value"] = round(lag)
    checks["total_error_rate_lt_1pct"] = (
        (report.total_errors / max(report.total_events_sent, 1)) * 100 < 1.0
    )

    report.sla_details = checks
    report.sla_passed = all(v for k, v in checks.items()
                            if isinstance(v, bool))
    status = "PASSED ✓" if report.sla_passed else "FAILED ✗"
    print(f"\n  SLA результат: {status}")
    for k, v in checks.items():
        print(f"    {k}: {v}")


# ──────────────────────────────────────────────────────────────
#  Режим только отчёта (--report)
# ──────────────────────────────────────────────────────────────

def report_only(args):
    print("\n[REPORT] Сбор метрик из Prometheus...")
    metrics = collect_prometheus_metrics(args.prometheus)

    # Количество записей в ClickHouse для базовой проверки полноты данных
    try:
        resp = requests.post(
            args.clickhouse,
            params={
                "query": "SELECT count() AS cnt FROM payments.payment_history FORMAT JSON",
                "database": "payments",
            },
            timeout=5,
        )
        data = resp.json()
        metrics["clickhouse_history_rows"] = int(
            data["data"][0]["cnt"]
        )
        print(f"    clickhouse_history_rows = {metrics['clickhouse_history_rows']}")
    except Exception as e:
        print(f"    [CH] Не удалось получить кол-во строк: {e}")

    output = {
        "report_time": datetime.now(timezone.utc).isoformat(),
        "prometheus_metrics": metrics,
    }
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        print(f"\n  Отчёт сохранён в {args.output}")
    else:
        print(json.dumps(output, ensure_ascii=False, indent=2))


# ──────────────────────────────────────────────────────────────
#  CLI
# ──────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Нагрузочное тестирование NRT Payment Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    sub = p.add_subparsers(dest="mode")

    # baseline
    bl = sub.add_parser("baseline", help="TC-C01: постоянная нагрузка")
    bl.add_argument("--rps", type=float, default=200,
                    help="Целевой RPS (default: 200)")
    bl.add_argument("--duration", type=float, default=300,
                    help="Длительность в секундах (default: 300)")
    bl.add_argument("--prometheus", default=PROMETHEUS_URL)
    bl.add_argument("--output", default=None, help="Путь для JSON-отчёта")

    # ramp
    rm = sub.add_parser("ramp", help="TC-C02: нарастающая нагрузка")
    rm.add_argument("--rps-start", type=float, default=200)
    rm.add_argument("--rps-peak", type=float, default=2000)
    rm.add_argument("--ramp-steps", type=int, default=3)
    rm.add_argument("--step-duration", type=float, default=120)
    rm.add_argument("--prometheus", default=PROMETHEUS_URL)
    rm.add_argument("--output", default=None)

    # report
    rp = sub.add_parser("report", help="Только сбор метрик без нагрузки")
    rp.add_argument("--prometheus", default=PROMETHEUS_URL)
    rp.add_argument("--clickhouse", default=CLICKHOUSE_URL)
    rp.add_argument("--output", default=None)

    # legacy flat args (для обратной совместимости с примерами в docs)
    p.add_argument("--scenario", choices=["baseline", "ramp"],
                   help="(legacy) сценарий — используйте subcommand вместо этого")
    p.add_argument("--rps", type=float, default=200)
    p.add_argument("--duration", type=float, default=300)
    p.add_argument("--rps-start", type=float, default=200)
    p.add_argument("--rps-peak", type=float, default=2000)
    p.add_argument("--ramp-steps", type=int, default=3)
    p.add_argument("--step-duration", type=float, default=120)
    p.add_argument("--report", action="store_true")
    p.add_argument("--prometheus", default=PROMETHEUS_URL)
    p.add_argument("--clickhouse", default=CLICKHOUSE_URL)
    p.add_argument("--output", default=None)

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

    # Определяем режим запуска
    mode = getattr(args, "mode", None) or (
        "report" if getattr(args, "report", False)
        else getattr(args, "scenario", None)
    )

    if mode == "baseline":
        report = scenario_baseline(args)
    elif mode == "ramp":
        report = scenario_ramp(args)
    elif mode == "report":
        report_only(args)
        return
    else:
        parser.print_help()
        sys.exit(1)

    # Сохраняем отчёт
    output_path = getattr(args, "output", None)
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            data = asdict(report)
            # StepResult.latencies_ms может быть очень большим — оставим только статистику
            for s in data.get("steps", []):
                n = len(s.get("latencies_ms", []))
                s["latencies_count"] = n
                s.pop("latencies_ms", None)
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"\nОтчёт сохранён в {output_path}")

    sys.exit(0 if report.sla_passed else 1)


if __name__ == "__main__":
    main()
