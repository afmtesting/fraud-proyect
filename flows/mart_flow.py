import os
import subprocess
from pathlib import Path
from datetime import datetime

from prefect import flow, task

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_ROOT / "fraud_dbt"
DBT_EXE = PROJECT_ROOT / "venv" / "Scripts" / "dbt.exe"


def _log_dir(dt: str) -> Path:
    d = PROJECT_ROOT / "logs" / f"dt={dt}"
    d.mkdir(parents=True, exist_ok=True)
    return d


@task(retries=2, retry_delay_seconds=10)
def run_cmd(cmd: list[str], cwd: Path, env: dict, log_file: str):
    dt = env.get("DT") or datetime.now().strftime("%Y-%m-%d")
    log_path = _log_dir(dt) / log_file

    p = subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True
    )

    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"\n===== {datetime.now().isoformat()} | CMD: {' '.join(cmd)} | CWD: {cwd} =====\n")
        if p.stdout:
            f.write("\n[STDOUT]\n" + p.stdout + "\n")
        if p.stderr:
            f.write("\n[STDERR]\n" + p.stderr + "\n")

    if p.returncode != 0:
        raise RuntimeError(f"CMD failed: {' '.join(cmd)}\nRevisa log: {log_path}")
    return p.stdout


@flow(name="fraud_mart_flow")
def mart_flow(dt: str):
    env = os.environ.copy()
    env["DT"] = dt
    env["PROJECT_ROOT"] = str(PROJECT_ROOT)

    if not (DBT_DIR / "dbt_project.yml").exists():
        raise RuntimeError(f"No existe dbt_project.yml en: {DBT_DIR}")

    dbt_cmd = str(DBT_EXE) if DBT_EXE.exists() else "dbt"

    run_cmd([dbt_cmd, "run", "-s", "mart+"], cwd=DBT_DIR, env=env, log_file="flows_mart_flow.py.log")
    return "OK"


if __name__ == "__main__":
    dt = os.environ.get("DT") or datetime.now().strftime("%Y-%m-%d")
    mart_flow(dt)
