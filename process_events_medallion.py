import os
import json
import re
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text

PG_URL = os.environ.get("PG_URL") or "postgresql://fraud:fraudpass@localhost:5432/frauddb"
PROJECT_ROOT = Path(__file__).resolve().parent
CHUNK_SIZE = 1000


def _find_latest_events_file(bronze_dir: Path) -> Path | None:
    files = sorted(bronze_dir.glob("events_*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _extract_batch_id(filename: str) -> str:
    m = re.search(r"batch=(\d{8}_\d{6})", filename, re.IGNORECASE)
    if m:
        return m.group(1)
    m2 = re.search(r"events_(\d{8}_\d{6})\.jsonl$", filename, re.IGNORECASE)
    if m2:
        return m2.group(1)
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _ensure_quarantine_table(conn):
    conn.execute(text("create schema if not exists raw;"))

    conn.execute(text("""
        create table if not exists raw.transaction_events_quarantine (
            id bigserial primary key,
            dt date not null,
            batch_id text not null,
            transaction_time bigint,
            payload text,
            source_file text,
            quarantine_reason text,
            quarantined_at timestamptz not null default now()
        );
    """))

    conn.execute(text("alter table raw.transaction_events_quarantine add column if not exists quarantine_reason text;"))
    conn.execute(text("alter table raw.transaction_events_quarantine add column if not exists quarantined_at timestamptz not null default now();"))
    # Backward-compatible schema alignment (handles older tables created without some columns)
    conn.execute(text("alter table raw.transaction_events_quarantine add column if not exists transaction_time bigint;"))
    conn.execute(text("alter table raw.transaction_events_quarantine add column if not exists payload text;"))
    conn.execute(text("alter table raw.transaction_events_quarantine add column if not exists source_file text;"))
    conn.execute(text("alter table raw.transaction_events_quarantine add column if not exists batch_id text;"))
    conn.execute(text("alter table raw.transaction_events_quarantine add column if not exists dt date;"))


def _upsert_batch_control(conn, dt: str, batch_id: str, source_file: str, rows_valid: int, rows_quarantined: int, status: str, error_message: str | None):
    conn.execute(text("""
        insert into raw.batch_control
            (batch_id, source, dt, source_file, rows_valid, rows_quarantined, status, error_message, load_ts, updated_ts)
        values
            (:batch_id, 'events', cast(:dt as date), :source_file, :rows_valid, :rows_quarantined, :status, :error_message, now(), now())
        on conflict (batch_id, source, dt)
        do update set
            source_file = excluded.source_file,
            rows_valid = excluded.rows_valid,
            rows_quarantined = excluded.rows_quarantined,
            status = excluded.status,
            error_message = excluded.error_message,
            updated_ts = now();
    """), {
        "batch_id": batch_id,
        "dt": dt,
        "source_file": source_file,
        "rows_valid": rows_valid,
        "rows_quarantined": rows_quarantined,
        "status": status,
        "error_message": error_message,
    })


def _validate_event(obj: dict):
    if "transaction_time" not in obj:
        return False, "missing transaction_time"
    if "payload" not in obj:
        return False, "missing payload"

    tt = obj.get("transaction_time")
    if tt is None:
        return False, "transaction_time is null"

    try:
        tt = int(tt)
    except Exception:
        return False, "transaction_time not int"

    if tt < 0:
        return False, "transaction_time < 0"

    payload = obj.get("payload")
    if payload is None or (isinstance(payload, str) and payload.strip() == ""):
        return False, "payload empty"

    if isinstance(payload, dict):
        rs = payload.get("risk_score")
        if rs is not None:
            try:
                rs = float(rs)
            except Exception:
                return False, "risk_score not numeric"
            if rs < 0 or rs > 1:
                return False, "risk_score out of [0,1]"
        return True, None

    if isinstance(payload, str):
        try:
            p = json.loads(payload)
            rs = p.get("risk_score")
            if rs is not None:
                rs = float(rs)
                if rs < 0 or rs > 1:
                    return False, "risk_score out of [0,1]"
        except Exception:
            return False, "payload not json"
        return True, None

    return False, "payload invalid type"


def main():
    dt = os.environ.get("DT")
    if not dt:
        raise ValueError("Debe definir variable de entorno DT. Ej: $env:DT='2026-02-13'")

    bronze_dir = PROJECT_ROOT / "data" / "bronze" / "events" / f"dt={dt}"
    if not bronze_dir.exists():
        print(f"No existe carpeta: {bronze_dir}")
        return

    jsonl_path = _find_latest_events_file(bronze_dir)
    if not jsonl_path:
        print("No hay archivos para procesar")
        return

    source_file = jsonl_path.name
    batch_id = _extract_batch_id(source_file)

    quarantine_dir = PROJECT_ROOT / "data" / "quarantine" / "events" / f"dt={dt}"
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    quarantine_jsonl = quarantine_dir / f"events_quarantine_{batch_id}.jsonl"

    engine = create_engine(PG_URL)

    valid_rows = []
    bad_rows = []
    quarantined_count = 0
    valid_count = 0

    try:
        with engine.begin() as conn:
            _ensure_quarantine_table(conn)
            # marca STARTED (opcional)
            _upsert_batch_control(conn, dt, batch_id, source_file, 0, 0, "STARTED", None)

        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    obj = json.loads(line)
                except Exception:
                    bad_rows.append(
                        {
                            "dt": dt,
                            "batch_id": batch_id,
                            "transaction_time": None,
                            "payload": line,
                            "source_file": source_file,
                            "quarantine_reason": "invalid json line",
                        }
                    )
                    quarantined_count += 1
                    continue

                ok, reason = _validate_event(obj)

                if ok:
                    payload = obj.get("payload")
                    payload_str = json.dumps(payload) if isinstance(payload, dict) else str(payload)

                    valid_rows.append(
                        {
                            "dt": dt,
                            "batch_id": batch_id,
                            "transaction_time": int(obj.get("transaction_time")),
                            "payload": payload_str,
                            "source_file": source_file,
                        }
                    )
                    valid_count += 1
                else:
                    payload = obj.get("payload")
                    payload_str = json.dumps(payload) if isinstance(payload, dict) else ("" if payload is None else str(payload))

                    bad_rows.append(
                        {
                            "dt": dt,
                            "batch_id": batch_id,
                            "transaction_time": obj.get("transaction_time"),
                            "payload": payload_str,
                            "source_file": source_file,
                            "quarantine_reason": reason,
                        }
                    )
                    quarantined_count += 1

                if len(valid_rows) >= CHUNK_SIZE:
                    pd.DataFrame(valid_rows).to_sql(
                        "transaction_events",
                        schema="raw",
                        con=engine,
                        if_exists="append",
                        index=False,
                        method="multi",
                    )
                    valid_rows.clear()

                if len(bad_rows) >= CHUNK_SIZE:
                    pd.DataFrame(bad_rows).to_sql(
                        "transaction_events_quarantine",
                        schema="raw",
                        con=engine,
                        if_exists="append",
                        index=False,
                        method="multi",
                    )
                    with open(quarantine_jsonl, "a", encoding="utf-8") as qf:
                        for r in bad_rows:
                            qf.write(json.dumps(r, ensure_ascii=False) + "\n")
                    bad_rows.clear()

        if valid_rows:
            pd.DataFrame(valid_rows).to_sql(
                "transaction_events",
                schema="raw",
                con=engine,
                if_exists="append",
                index=False,
                method="multi",
            )

        if bad_rows:
            pd.DataFrame(bad_rows).to_sql(
                "transaction_events_quarantine",
                schema="raw",
                con=engine,
                if_exists="append",
                index=False,
                method="multi",
            )
            with open(quarantine_jsonl, "a", encoding="utf-8") as qf:
                for r in bad_rows:
                    qf.write(json.dumps(r, ensure_ascii=False) + "\n")

        with engine.begin() as conn:
            _upsert_batch_control(conn, dt, batch_id, source_file, valid_count, quarantined_count, "LOADED", None)

        if quarantined_count > 0:
            print(f"OK events: valid={valid_count} quarantined={quarantined_count} file={source_file}")
            print(f"QUARANTINE JSONL: {quarantine_jsonl}")
        else:
            print(f"OK events: valid={valid_count} quarantined=0 file={source_file}")

    except Exception as e:
        # registra FAILED
        try:
            with engine.begin() as conn:
                _upsert_batch_control(conn, dt, batch_id, source_file, valid_count, quarantined_count, "FAILED", f"{type(e).__name__}: {e}")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()