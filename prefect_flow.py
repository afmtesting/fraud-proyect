from __future__ import annotations

import os
import subprocess
from datetime import datetime
from pathlib import Path

from prefect import flow, task


def _project_root() -> Path:
    """
    Fuerza un root estable para filesystem (data/, logs/, etc.).
    1) Si existe env PROJECT_ROOT, úsalo.
    2) Si no, usa el path del archivo (fallback).
    """
    env_root = os.environ.get("PROJECT_ROOT")
    if env_root:
        return Path(env_root).resolve()
    return Path(__file__).resolve().parent


def run_command(cmd: str, cwd: Path) -> None:
    print(f"[CMD] {cmd}")
    print(f"[CWD] {cwd}")

    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        shell=True,
        capture_output=True,
        text=True,
    )

    if proc.stdout:
        print("[STDOUT]")
        print(proc.stdout)

    if proc.stderr:
        print("[STDERR]")
        print(proc.stderr)

    if proc.returncode != 0:
        raise RuntimeError(f"Command failed (exit={proc.returncode}): {cmd}")



@task(retries=2, retry_delay_seconds=30)
def start_docker(project_root: str) -> None:
    run_command("docker compose up -d", cwd=Path(project_root))


@task(retries=2, retry_delay_seconds=30)
def run_pipeline(project_root: str, dt: str, simulate: str) -> None:
    os.environ["DT"] = dt
    os.environ["SIMULATE"] = simulate
    os.environ["PROJECT_ROOT"] = project_root

    root = Path(project_root)

    # Asegura que todo lo relativo caiga en tu repo (no en temp)
    os.chdir(str(root))
    print(f"[INFO] CWD={Path.cwd()}")
    print(f"[INFO] PROJECT_ROOT={root}")

    py = root / "venv" / "Scripts" / "python.exe"
    if not py.exists():
        raise FileNotFoundError(f"Python venv not found: {py}")

    run_command(f'"{py}" init_batch_control.py', cwd=root)
    run_command(f'"{py}" run_daily.py', cwd=root)


@task(retries=2, retry_delay_seconds=30)
def run_dbt(project_root: str) -> None:
    root = Path(project_root)
    dbt = root / "venv" / "Scripts" / "dbt.exe"
    if not dbt.exists():
        raise FileNotFoundError(f"dbt not found in venv: {dbt}")

    dbt_dir = root / "fraud_dbt"
    if not dbt_dir.exists():
        raise FileNotFoundError(f"fraud_dbt folder not found: {dbt_dir}")

    run_command(f'"{dbt}" deps', cwd=dbt_dir)
    run_command(f'"{dbt}" build', cwd=dbt_dir)


@flow(name="fraud-daily-pipeline")
def fraud_daily_flow(dt: str | None = None, simulate: str = "1", project_root: str | None = None) -> None:
    root = Path(project_root).resolve() if project_root else _project_root()
    dt = dt or datetime.now().strftime("%Y-%m-%d")

    print(f"[FLOW] Using project_root={root}")
    print(f"[FLOW] Using dt={dt} simulate={simulate}")

    start_docker(str(root))
    run_pipeline(str(root), dt, simulate)
    run_dbt(str(root))


if __name__ == "__main__":
    fraud_daily_flow()
