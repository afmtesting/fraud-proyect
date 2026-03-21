import os
from sqlalchemy import create_engine, text

PG_URL = "postgresql+psycopg2://fraud:fraudpass@localhost:5432/frauddb"

def _fail(msg: str):
    raise RuntimeError(f"[DQ FAILED] {msg}")

def run_dq_gate():
    """
    Quality gate "realista" para ingesta diaria variable.

    Chequeos:
      - Volumen dentro de un rango esperado (min..max)
      - Amount: no null, > 0
      - Time: dentro de [0, 86399]
      - Fraud rate dentro de un rango (min..max)
    """

    # --- Umbrales configurables (env vars) ---
    expected_min = int(os.environ.get("DQ_EXPECTED_MIN_ROWS", "5000"))
    expected_max = int(os.environ.get("DQ_EXPECTED_MAX_ROWS", "60000"))

    fraud_min = float(os.environ.get("DQ_FRAUD_MIN", "0.001"))   # 0.1%
    fraud_max = float(os.environ.get("DQ_FRAUD_MAX", "0.050"))   # 5%

    max_null_amount = int(os.environ.get("DQ_MAX_NULL_AMOUNT", "0"))
    max_nonpos_amount = int(os.environ.get("DQ_MAX_NONPOS_AMOUNT", "0"))
    max_bad_time = int(os.environ.get("DQ_MAX_BAD_TIME", "0"))

    engine = create_engine(PG_URL)

    with engine.connect() as conn:
        # 1) Volumen
        n_tx = conn.execute(text("select count(*) from stg.creditcard")).scalar_one()
        if n_tx < expected_min or n_tx > expected_max:
            _fail(f"volumen fuera de rango: {n_tx} (esperado {expected_min}..{expected_max})")

        # 2) Amount null / <= 0
        null_amount = conn.execute(
            text("select count(*) from stg.creditcard where amount is null")
        ).scalar_one()
        if null_amount > max_null_amount:
            _fail(f"amount nulls: {null_amount} (max={max_null_amount})")

        nonpos_amount = conn.execute(
            text("select count(*) from stg.creditcard where amount <= 0")
        ).scalar_one()
        if nonpos_amount > max_nonpos_amount:
            _fail(f"amount <= 0: {nonpos_amount} (max={max_nonpos_amount})")

        # 3) Time dentro de [0, 86399]
        bad_time = conn.execute(
            text("select count(*) from stg.creditcard where time < 0 or time > 86399")
        ).scalar_one()
        if bad_time > max_bad_time:
            _fail(f"time fuera de rango [0,86399]: {bad_time} (max={max_bad_time})")

        # 4) Fraud rate razonable
        fraud_rate = conn.execute(
            text("select avg(class::numeric) from stg.creditcard")
        ).scalar_one()
        if fraud_rate is None:
            _fail("fraud_rate es NULL (tabla vacía o class no calculable)")
        if fraud_rate < fraud_min or fraud_rate > fraud_max:
            _fail(f"fraud_rate fuera de rango: {fraud_rate:.6f} (esperado {fraud_min}..{fraud_max})")

    print(
        "[DQ PASSED] "
        f"rows={n_tx} "
        f"fraud_rate={fraud_rate:.6f} "
        f"null_amount={null_amount} "
        f"nonpos_amount={nonpos_amount} "
        f"bad_time={bad_time} "
        f"(expected_rows={expected_min}..{expected_max})"
    )
