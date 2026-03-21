import pandas as pd
from sqlalchemy import create_engine, text

CSV_PATH = r"data\raw\creditcard.csv"
PG_URL = "postgresql+psycopg2://fraud:fraudpass@localhost:5432/frauddb"

def main():
    df = pd.read_csv(CSV_PATH)
    df.columns = [c.strip().lower() for c in df.columns]

    engine = create_engine(PG_URL)

    with engine.begin() as conn:
        conn.exec_driver_sql("create schema if not exists stg;")

        # Crear tabla si no existe (estructura basada en el CSV)
        # Si ya existe, solo vaciarla para no romper dependencias de dbt
        exists = conn.execute(text("""
            select exists (
              select 1
              from information_schema.tables
              where table_schema='stg' and table_name='creditcard'
            )
        """)).scalar_one()

        if not exists:
            # crea tabla con pandas (una vez)
            df.head(0).to_sql("creditcard", engine, schema="stg", if_exists="replace", index=False)
        else:
            conn.execute(text("truncate table stg.creditcard;"))

    # Insertar datos
    df.to_sql(
        "creditcard",
        engine,
        schema="stg",
        if_exists="append",
        index=False,
        chunksize=50000,
        method="multi",
    )

    print("OK: cargado stg.creditcard, filas:", len(df))

if __name__ == "__main__":
    main()
