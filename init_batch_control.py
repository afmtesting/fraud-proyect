import os
from sqlalchemy import create_engine, text

# Usa PG_URL si existe; si no, usa default
PG_URL = os.environ.get("PG_URL") or "postgresql+psycopg2://fraud:fraudpass@localhost:5432/frauddb"

SQL_STEPS = [
    # 1) Schema
    """
    create schema if not exists raw;
    """,

    # 2) Tabla mínima
    """
    create table if not exists raw.batch_control (
        id bigserial primary key
    );
    """,

    # 3) Migraciones incrementales (columnas)
    """
    alter table raw.batch_control
        add column if not exists batch_id text;
    """,
    """
    alter table raw.batch_control
        add column if not exists source text;
    """,
    """
    alter table raw.batch_control
        add column if not exists pipeline text;
    """,
    """
    alter table raw.batch_control
        add column if not exists dataset text;
    """,
    """
    alter table raw.batch_control
        add column if not exists dt date;
    """,
    """
    alter table raw.batch_control
        add column if not exists source_file text;
    """,
    """
    alter table raw.batch_control
        add column if not exists rows_expected bigint;
    """,
    """
    alter table raw.batch_control
        add column if not exists rows_valid bigint not null default 0;
    """,
    """
    alter table raw.batch_control
        add column if not exists rows_quarantined bigint not null default 0;
    """,
    """
    alter table raw.batch_control
        add column if not exists status text not null default 'STARTED';
    """,
    """
    alter table raw.batch_control
        add column if not exists error_message text;
    """,
    """
    alter table raw.batch_control
        add column if not exists load_ts timestamptz not null default now();
    """,
    """
    alter table raw.batch_control
        add column if not exists updated_ts timestamptz not null default now();
    """,
]

def main() -> None:
    engine = create_engine(PG_URL, future=True)
    with engine.begin() as conn:
        for i, sql in enumerate(SQL_STEPS, start=1):
            conn.execute(text(sql))
            print(f"[OK] step {i}")

    print("[DONE] batch_control ready")

if __name__ == "__main__":
    main()
