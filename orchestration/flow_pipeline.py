import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime
from prefect import flow, task

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT") or Path(__file__).resolve().parents[1])
DBT_DIR = PROJECT_ROOT / "fraud_dbt"

# Asegura imports tipo "orchestration.*"
sys.path.insert(0, str(PROJECT_ROOT))

PY = sys.executable  # python del venv

def _dt():
    # DT opcional (yyyy-mm-dd). Si no viene, usa hoy.
    return os.environ.get("DT") or datetime.now().strftime("%Y-%m-%d")

@task(retries=2, retry_delay_seconds=5)
def promote_to_bronze():
    env = os.environ.copy()
    env["DT"] = _dt()
    subprocess.run([PY, str(PROJECT_ROOT / "orchestration" / "promote_to_bronze.py")], check=True, env=env)

@task(retries=2, retry_delay_seconds=5)
def process_creditcard():
    env = os.environ.copy()
    env["DT"] = _dt()
    subprocess.run([PY, str(PROJECT_ROOT / "process_creditcard_medallion.py")], check=True, env=env)

@task(retries=2, retry_delay_seconds=5)
def process_events():
    env = os.environ.copy()
    env["DT"] = _dt()
    subprocess.run([PY, str(PROJECT_ROOT / "process_events_medallion.py")], check=True, env=env)

@task
def dq_gate():
    # DQ gate sobre lo que quedó en DB
    from orchestration.dq_gate import run_dq_gate
    run_dq_gate()

@task
def dbt_build():
    subprocess.run(["dbt", "build"], check=True, cwd=str(DBT_DIR))

@flow(name="fraud_medallion_pipeline")
def fraud_pipeline():
    # 1) Mover LANDING -> BRONZE (por dt)
    promote_to_bronze()

    # 2) BRONZE -> SILVER (parquet) + carga a DB
    process_creditcard()
    process_events()

    # 3) DQ + Gold
    dq_gate()
    dbt_build()

if __name__ == "__main__":
    fraud_pipeline()