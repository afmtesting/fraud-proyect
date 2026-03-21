import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

from prefect import flow, task

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PY = sys.executable  # python del venv


def _log_dir(dt: str) -> Path:
    d = PROJECT_ROOT / "logs" / f"dt={dt}"
    d.mkdir(parents=True, exist_ok=True)
    return d


@task(retries=2, retry_delay_seconds=10)
def run_cmd(cmd: list[str], env: dict, log_file: str):
    dt = env.get("DT") or datetime.now().strftime("%Y-%m-%d")
    log_path = _log_dir(dt) / log_file

    p = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        env=env,
        capture_output=True,
        text=True
    )

    # Persistimos stdout/stderr SIEMPRE para trazabilidad
    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"\n===== {datetime.now().isoformat()} | CMD: {' '.join(cmd)} =====\n")
        if p.stdout:
            f.write("\n[STDOUT]\n" + p.stdout + "\n")
        if p.stderr:
            f.write("\n[STDERR]\n" + p.stderr + "\n")

    if p.returncode != 0:
        raise RuntimeError(
            f"CMD failed: {' '.join(cmd)}\nRevisa log: {log_path}"
        )

    return p.stdout


@flow(name="fraud_ingest_flow")
def ingest_flow(dt: str):
    env = os.environ.copy()
    env["DT"] = dt
    env["PROJECT_ROOT"] = str(PROJECT_ROOT)

    # 0) (Opcional) simular llegada en landing si SIMULATE=1
    if env.get("SIMULATE", "0") == "1":
        run_cmd([PY, "simulate_arrival.py"], env, "00_simulate_arrival.log")

    # 1) Landing -> Bronze (idempotente)
    run_cmd([PY, str(PROJECT_ROOT / "orchestration" / "promote_to_bronze.py")], env, "01_promote_to_bronze.log")

    # 2) Bronze -> DB (raw) + quarantine
    run_cmd([PY, "process_events_medallion.py"], env, "02_process_events.log")
    run_cmd([PY, "process_creditcard_medallion.py"], env, "03_process_creditcard.log")

    return "OK"


if __name__ == "__main__":
    dt = os.environ.get("DT") or datetime.now().strftime("%Y-%m-%d")
    ingest_flow(dt)
