import os
import random
import json
import re
from pathlib import Path
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

PG_URL = "postgresql+psycopg2://fraud:fraudpass@localhost:5432/frauddb"

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT") or Path(__file__).resolve().parent)
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "events"
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "events"

def _batch_id_from_ts(run_ts: str) -> str:
    # run_ts = YYYYMMDD_HHMMSS
    return run_ts

def main():
    engine = create_engine(PG_URL)

    # Fecha de proceso (para partición). Si no viene, usa hoy.
    dt = os.environ.get("DT") or datetime.now().strftime("%Y-%m-%d")

    # Tomamos muestra de transacciones para asociar time_sec
    df = pd.read_sql("select time_sec from public.stg_creditcard limit 5000", engine)

    channels = ["web", "mobile", "pos"]
    devices = ["ios", "android", "windows", "mac"]
    merchants = ["amazon", "walmart", "target", "bestbuy", "ebay"]

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_id = _batch_id_from_ts(run_ts)

    # Particionado por fecha (medallion-style)
    bronze_dt_dir = BRONZE_DIR / f"dt={dt}"
    silver_dt_dir = SILVER_DIR / f"dt={dt}"
    bronze_dt_dir.mkdir(parents=True, exist_ok=True)
    silver_dt_dir.mkdir(parents=True, exist_ok=True)

    bronze_filename = f"events_dt={dt}_batch={batch_id}.jsonl"
    silver_filename = "events.parquet"

    bronze_path = bronze_dt_dir / bronze_filename
    silver_path = silver_dt_dir / silver_filename

    # 1) BRONZE: escribir JSONL (1 json por línea)
    records = []
    with bronze_path.open("w", encoding="utf-8") as f:
        for t in df["time_sec"]:
            payload = {
                "dt": dt,  # clave para trazabilidad
                "batch_id": batch_id,
                "source_file": bronze_filename,
                "channel": random.choice(channels),
                "device": random.choice(devices),
                "merchant": random.choice(merchants),
                "risk_score": round(random.uniform(0, 1), 3),
            }
            row = {
                "transaction_time": int(t),   # segundos dentro del día (0..86399)
                "payload": payload
            }
            f.write(json.dumps(row) + "\n")
            records.append(row)

    print("BRONZE JSONL:", bronze_path)

    # 2) SILVER: normalizar + Parquet (con dt)
    events_df = pd.DataFrame({
        "dt": [dt] * len(records),
        "time_sec": [r["transaction_time"] for r in records],
        "channel": [r["payload"]["channel"] for r in records],
        "device": [r["payload"]["device"] for r in records],
        "merchant": [r["payload"]["merchant"] for r in records],
        "risk_score": [r["payload"]["risk_score"] for r in records],
        "batch_id": [batch_id] * len(records),
        "source_file": [bronze_filename] * len(records),
        "run_ts": [run_ts] * len(records),
    })

    events_df.to_parquet(silver_path, index=False)
    print("SILVER PARQUET:", silver_path)

    # 3) Cargar a Postgres RAW (JSONB) + columna dt real (gobernanza / joins)
    with engine.begin() as conn:
        conn.exec_driver_sql("create schema if not exists raw;")
        conn.exec_driver_sql("""
            create table if not exists raw.transaction_events (
                id serial primary key,
                dt date,
                transaction_time bigint,
                payload jsonb
            );
        """)

    db_df = pd.DataFrame({
        "dt": events_df["dt"],
        "transaction_time": events_df["time_sec"].astype(int),
        "payload": [
            json.dumps({
                "dt": dt,
                "batch_id": batch_id,
                "source_file": bronze_filename,
                "channel": c,
                "device": d,
                "merchant": m,
                "risk_score": float(r),
            })
            for c, d, m, r in zip(
                events_df["channel"],
                events_df["device"],
                events_df["merchant"],
                events_df["risk_score"],
            )
        ],
    })

    db_df.to_sql(
        "transaction_events",
        engine,
        schema="raw",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )

    print(f"Eventos insertados: {len(db_df)} DT: {dt} SOURCE: {bronze_filename}")

if __name__ == "__main__":
    main()