import os
import re
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch


PG_DSN = os.environ.get("PG_URL") or "postgresql://fraud:fraudpass@localhost:5432/frauddb"
PROJECT_ROOT = Path(__file__).resolve().parent
CHUNK_SIZE = 500


def _latest_file(folder: Path, pattern: str) -> Path:
    files = sorted(folder.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise RuntimeError(f"No hay archivos {pattern} en {folder}")
    return files[0]


def _extract_batch_id(filename: str) -> str:
    m = re.search(r"creditcard_(\d{8}_\d{6})\.csv$", filename, re.IGNORECASE)
    if m:
        return f"creditcard_{m.group(1)}"
    m2 = re.search(r"creditcard_dt=\d{4}-\d{2}-\d{2}_batch=(\d{8}_\d{6})\.csv$", filename, re.IGNORECASE)
    if m2:
        return f"creditcard_{m2.group(1)}"
    return "creditcard_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _ensure_quarantine_table(cur):
    cur.execute("create schema if not exists raw;")
    cur.execute(
        """
        create table if not exists raw.creditcard_batches_quarantine (
            id bigserial primary key,
            dt date not null,
            batch_id text not null,
            time bigint,
            amount numeric(12,2),
            class smallint,
            v1 double precision,
            v2 double precision,
            v3 double precision,
            v4 double precision,
            v5 double precision,
            v6 double precision,
            v7 double precision,
            v8 double precision,
            v9 double precision,
            v10 double precision,
            v11 double precision,
            v12 double precision,
            v13 double precision,
            v14 double precision,
            v15 double precision,
            v16 double precision,
            v17 double precision,
            v18 double precision,
            v19 double precision,
            v20 double precision,
            v21 double precision,
            v22 double precision,
            v23 double precision,
            v24 double precision,
            v25 double precision,
            v26 double precision,
            v27 double precision,
            v28 double precision,
            source_file text,
            quarantine_reason text,
            quarantined_at timestamptz not null default now()
        );
        """
    )

    # Backward-compatible schema alignment (handles older tables created without some columns)
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists dt date;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists batch_id text;")
    cur.execute('alter table raw.creditcard_batches_quarantine add column if not exists "time" bigint;')
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists amount numeric(12,2);")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists class smallint;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists source_file text;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v1 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v2 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v3 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v4 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v5 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v6 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v7 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v8 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v9 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v10 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v11 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v12 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v13 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v14 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v15 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v16 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v17 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v18 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v19 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v20 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v21 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v22 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v23 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v24 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v25 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v26 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v27 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists v28 double precision;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists quarantine_reason text;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists quarantined_at timestamptz not null default now();")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists quarantine_reason text;")
    cur.execute("alter table raw.creditcard_batches_quarantine add column if not exists quarantined_at timestamptz not null default now();")


def _upsert_batch_control(cur, dt: str, batch_id: str, source_file: str, rows_valid: int, rows_quarantined: int, status: str, error_message: str | None):
    cur.execute(
        """
        insert into raw.batch_control
            (batch_id, source, dt, source_file, rows_valid, rows_quarantined, status, error_message, load_ts, updated_ts)
        values
            (%s, 'creditcard', cast(%s as date), %s, %s, %s, %s, %s, now(), now())
        on conflict (batch_id, source, dt)
        do update set
            source_file = excluded.source_file,
            rows_valid = excluded.rows_valid,
            rows_quarantined = excluded.rows_quarantined,
            status = excluded.status,
            error_message = excluded.error_message,
            updated_ts = now();
        """,
        (batch_id, dt, source_file, rows_valid, rows_quarantined, status, error_message),
    )


def _validate_row(df_cols, row_tuple):
    def get(col):
        return row_tuple[df_cols.get_loc(col)]

    t = get("time")
    a = get("amount")
    c = get("class")

    if pd.isna(t):
        return False, "time is null"
    if pd.isna(a):
        return False, "amount is null"
    if pd.isna(c):
        return False, "class is null"

    try:
        t = int(t)
    except Exception:
        return False, "time not int"

    try:
        a = float(a)
    except Exception:
        return False, "amount not numeric"

    try:
        c = int(c)
    except Exception:
        return False, "class not int"

    if t < 0:
        return False, "time < 0"
    if t > 86399:
        return False, "time > 86399"
    if a <= 0:
        return False, "amount <= 0"
    if c not in (0, 1):
        return False, "class not in (0,1)"

    return True, None


def main():
    dt = os.environ.get("DT")
    if not dt:
        raise ValueError("Debe definir variable de entorno DT. Ej: $env:DT='2026-02-13'")

    bronze_dir = PROJECT_ROOT / "data" / "bronze" / "creditcard" / f"dt={dt}"
    if not bronze_dir.exists():
        print(f"No existe carpeta: {bronze_dir}")
        return

    csv_path = _latest_file(bronze_dir, "creditcard_*.csv")
    source_file = csv_path.name
    batch_id = _extract_batch_id(source_file)

    df = pd.read_csv(csv_path)
    df.columns = [str(c).strip().lower() for c in df.columns]

    required = ["time", "amount", "class"] + [f"v{i}" for i in range(1, 29)]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Faltan columnas en CSV: {missing}")

    quarantine_dir = PROJECT_ROOT / "data" / "quarantine" / "creditcard" / f"dt={dt}"
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    quarantine_csv_path = quarantine_dir / f"creditcard_quarantine_{batch_id}.csv"

    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()

    _ensure_quarantine_table(cur)
    conn.commit()

    # STARTED
    _upsert_batch_control(cur, dt, batch_id, source_file, 0, 0, "STARTED", None)

    # Idempotencia: si re-ejecutas el MISMO batch_id, limpiamos lo previo para evitar duplicados
    cur.execute("delete from raw.creditcard_batches where dt = cast(%s as date) and batch_id = %s;", (dt, batch_id))
    cur.execute("delete from raw.creditcard_batches_quarantine where dt = cast(%s as date) and batch_id = %s;", (dt, batch_id))

    conn.commit()

    insert_valid_sql = """
        INSERT INTO raw.creditcard_batches (
            dt, batch_id, time, amount, class,
            v1, v2, v3, v4, v5, v6, v7, v8, v9, v10,
            v11, v12, v13, v14, v15, v16, v17, v18,
            v19, v20, v21, v22, v23, v24, v25, v26,
            v27, v28, source_file
        )
        VALUES (
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s
        )
    """

    insert_quarantine_sql = """
        INSERT INTO raw.creditcard_batches_quarantine (
            dt, batch_id, time, amount, class,
            v1, v2, v3, v4, v5, v6, v7, v8, v9, v10,
            v11, v12, v13, v14, v15, v16, v17, v18,
            v19, v20, v21, v22, v23, v24, v25, v26,
            v27, v28, source_file, quarantine_reason
        )
        VALUES (
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s
        )
    """

    valid_records = []
    bad_records = []
    bad_rows_for_csv = []

    cols = df.columns
    valid_count = 0
    quarantined_count = 0

    try:
        for row in df.itertuples(index=False, name=None):
            ok, reason = _validate_row(cols, row)

            time_val = None if pd.isna(row[cols.get_loc("time")]) else int(row[cols.get_loc("time")])
            amount_val = None if pd.isna(row[cols.get_loc("amount")]) else float(row[cols.get_loc("amount")])
            class_val = None if pd.isna(row[cols.get_loc("class")]) else int(row[cols.get_loc("class")])

            v_vals = []
            for i in range(1, 29):
                v = row[cols.get_loc(f"v{i}")]
                v_vals.append(None if pd.isna(v) else float(v))

            base_tuple = (
                dt,
                batch_id,
                time_val,
                amount_val,
                class_val,
                *v_vals,
                source_file,
            )

            if ok:
                valid_records.append(base_tuple)
                valid_count += 1
            else:
                bad_records.append((*base_tuple, reason))
                quarantined_count += 1
                bad_rows_for_csv.append(
                    {
                        "dt": dt,
                        "batch_id": batch_id,
                        "source_file": source_file,
                        "quarantine_reason": reason,
                        "time": time_val,
                        "amount": amount_val,
                        "class": class_val,
                        **{f"v{i}": v_vals[i - 1] for i in range(1, 29)},
                    }
                )

            if len(valid_records) >= CHUNK_SIZE:
                execute_batch(cur, insert_valid_sql, valid_records, page_size=CHUNK_SIZE)
                conn.commit()
                valid_records.clear()

            if len(bad_records) >= CHUNK_SIZE:
                execute_batch(cur, insert_quarantine_sql, bad_records, page_size=CHUNK_SIZE)
                conn.commit()
                bad_records.clear()

        if valid_records:
            execute_batch(cur, insert_valid_sql, valid_records, page_size=len(valid_records))
            conn.commit()

        if bad_records:
            execute_batch(cur, insert_quarantine_sql, bad_records, page_size=len(bad_records))
            conn.commit()

        # escribir CSV cuarentena
        if bad_rows_for_csv:
            pd.DataFrame(bad_rows_for_csv).to_csv(quarantine_csv_path, index=False)

        # LOADED
        _upsert_batch_control(cur, dt, batch_id, source_file, valid_count, quarantined_count, "LOADED", None)
        conn.commit()

    except Exception as e:
        # FAILED
        try:
            _upsert_batch_control(cur, dt, batch_id, source_file, valid_count, quarantined_count, "FAILED", f"{type(e).__name__}: {e}")
            conn.commit()
        except Exception:
            pass
        raise
    finally:
        cur.close()
        conn.close()

    if quarantined_count > 0:
        print(f"OK creditcard: valid={valid_count} quarantined={quarantined_count} file={source_file}")
        print(f"QUARANTINE CSV: {quarantine_csv_path}")
    else:
        print(f"OK creditcard: valid={valid_count} quarantined=0 file={source_file}")


if __name__ == "__main__":
    main()