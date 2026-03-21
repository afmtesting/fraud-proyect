import os
import sys
from datetime import datetime
from pathlib import Path

# --- bootstrap: asegura imports si ejecutas desde Task Scheduler / cualquier cwd ---
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from flows.ingest_flow import ingest_flow
from flows.transform_flow import transform_flow
from flows.mart_flow import mart_flow


def main():
    # si no pasas DT, usa hoy (local)
    dt = os.environ.get("DT") or datetime.now().strftime("%Y-%m-%d")
    os.environ["DT"] = dt

    print(f"[RUN] DT={dt}")

    ingest_flow(dt)
    transform_flow(dt)
    mart_flow(dt)

    print("[RUN] OK")


if __name__ == "__main__":
    main()
