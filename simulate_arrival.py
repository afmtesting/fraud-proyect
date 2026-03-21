import os
import json
import random
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT") or Path(__file__).resolve().parent)


def main():
    # Parámetros
    dt = os.environ.get("DT") or datetime.now().strftime("%Y-%m-%d")
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ===== Fault Injection (para cuarentena) =====
    # Recomendado: 0.005 a 0.02 (0.5% a 2%)
    bad_rate_csv = float(os.environ.get("BAD_RATE_CSV", "0.01"))
    bad_rate_events = float(os.environ.get("BAD_RATE_EVENTS", "0.01"))

    # ===== VOLUMEN VARIABLE REALISTA =====
    n_tx_env = os.environ.get("N_TX")
    if n_tx_env:
        n_tx = int(n_tx_env)  # override manual
    else:
        day = datetime.strptime(dt, "%Y-%m-%d").weekday()  # 0=lun ... 6=dom
        base_weekday = int(os.environ.get("BASE_WEEKDAY_TX", "18000"))  # lun-vie
        base_weekend = int(os.environ.get("BASE_WEEKEND_TX", "9000"))   # sáb-dom

        base = base_weekday if day < 5 else base_weekend

        # ruido +/- 20%
        noise = np.random.uniform(0.8, 1.2)
        n_tx = int(base * noise)

        # spike ocasional (5%) x 1.5 a 2.5
        if np.random.rand() < float(os.environ.get("SPIKE_PROB", "0.05")):
            n_tx = int(n_tx * np.random.uniform(1.5, 2.5))

        # límites de seguridad
        min_tx = int(os.environ.get("MIN_TX", "5000"))
        max_tx = int(os.environ.get("MAX_TX", "60000"))
        n_tx = max(min_tx, min(max_tx, n_tx))

    fraud_rate = float(os.environ.get("FRAUD_RATE", "0.007"))  # 0.7% default

    seed = os.environ.get("SEED")
    if seed is not None:
        np.random.seed(int(seed))
        random.seed(int(seed))

    # LANDING particionado por dt
    landing_cc_dir = PROJECT_ROOT / "data" / "landing" / "creditcard" / f"dt={dt}"
    landing_ev_dir = PROJECT_ROOT / "data" / "landing" / "events" / f"dt={dt}"
    landing_cc_dir.mkdir(parents=True, exist_ok=True)
    landing_ev_dir.mkdir(parents=True, exist_ok=True)

    # ====== GENERACIÓN CSV SINTÉTICO (mismo esquema que creditcard.csv) ======
    # time: segundos dentro del día (0..86399)
    time_sec = np.random.randint(0, 86400, size=n_tx)

    # class (fraude): Bernoulli con tasa fraud_rate
    is_fraud = (np.random.rand(n_tx) < fraud_rate).astype(int)

    # v1..v28: normal(0,1) para no-fraude; para fraude aplicamos un shift leve (señal)
    v = np.random.normal(0, 1, size=(n_tx, 28))
    fraud_idx = np.where(is_fraud == 1)[0]
    if len(fraud_idx) > 0:
        shift_dims = [0, 2, 4, 7, 10, 13, 17, 21, 25]  # índices 0-based => V1,V3,...
        v[fraud_idx[:, None], shift_dims] += np.random.normal(
            1.5, 0.4, size=(len(fraud_idx), len(shift_dims))
        )

    # amount: lognormal (montos positivos), fraude tiende a ser más alto
    amount = np.random.lognormal(mean=3.0, sigma=0.9, size=n_tx)
    if len(fraud_idx) > 0:
        amount[fraud_idx] *= np.random.uniform(1.5, 4.0, size=len(fraud_idx))

    df = pd.DataFrame({"time": time_sec})
    for i in range(28):
        df[f"v{i+1}"] = v[:, i]
    df["amount"] = np.round(amount, 2)
    df["class"] = is_fraud

    # ===== Fault Injection en CSV (para mandar a cuarentena) =====
    # Inyecta filas inválidas SIN romper el archivo completo.
    # Los loaders deben validar por fila y mandar a *_quarantine.
    injected_csv = 0
    if bad_rate_csv > 0:
        n_bad_csv = int(max(1, round(n_tx * bad_rate_csv))) if n_tx > 0 else 0
        n_bad_csv = min(n_bad_csv, max(1, n_tx // 100))  # hard cap ~1% si n_tx es enorme
        bad_idx = np.random.choice(df.index.values, size=n_bad_csv, replace=False)

        # Tipos de error controlados
        for i, idx in enumerate(bad_idx):
            kind = random.choice(["amount_negative", "class_invalid", "time_out_of_range", "amount_null"])
            if kind == "amount_negative":
                df.loc[idx, "amount"] = -abs(float(df.loc[idx, "amount"]))  # inválido
            elif kind == "class_invalid":
                df.loc[idx, "class"] = 3  # inválido (solo {0,1})
            elif kind == "time_out_of_range":
                df.loc[idx, "time"] = 999999  # inválido (fuera de 0..86399)
            elif kind == "amount_null":
                df.loc[idx, "amount"] = np.nan  # inválido (depende de tu validador)
            injected_csv += 1

    out_csv = landing_cc_dir / f"creditcard_dt={dt}_batch={batch_id}.csv"
    df.to_csv(out_csv, index=False)

    # ====== GENERACIÓN JSONL EVENTS (semi-structured) ======
    n_events = int(os.environ.get("N_EVENTS", "5000"))
    n_events = min(n_events, n_tx)

    sample_idx = np.random.choice(np.arange(n_tx), size=n_events, replace=False) if n_events > 0 else []
    channels = ["web", "mobile", "pos"]
    devices = ["ios", "android", "windows", "mac"]
    merchants = ["amazon", "walmart", "target", "bestbuy", "ebay", "ubereats", "spotify", "netflix"]

    injected_events = 0
    out_jsonl = landing_ev_dir / f"events_dt={dt}_batch={batch_id}.jsonl"
    with out_jsonl.open("w", encoding="utf-8") as f:
        for idx in sample_idx:
            # risk_score correlacionado: fraude tiende a score más alto
            base = float(np.clip(np.random.normal(0.35, 0.15), 0, 1))
            if int(df.loc[idx, "class"]) == 1:
                base = float(np.clip(base + np.random.uniform(0.25, 0.55), 0, 1))

            row = {
                "transaction_time": int(df.loc[idx, "time"]),
                "payload": {
                    "channel": random.choice(channels),
                    "device": random.choice(devices),
                    "merchant": random.choice(merchants),
                    "risk_score": round(base, 3),
                },
            }

            # ===== Fault Injection en JSONL =====
            # Inyecta algunas líneas inválidas:
            # - NOT_JSON (línea no parseable)
            # - missing transaction_time
            # - risk_score fuera de rango
            if bad_rate_events > 0 and random.random() < bad_rate_events:
                kind = random.choice(["not_json", "missing_time", "bad_score"])
                if kind == "not_json":
                    f.write("NOT_JSON\n")
                    injected_events += 1
                    continue
                elif kind == "missing_time":
                    row.pop("transaction_time", None)
                    injected_events += 1
                elif kind == "bad_score":
                    row["payload"]["risk_score"] = 1.7  # fuera de [0,1]
                    injected_events += 1

            f.write(json.dumps(row) + "\n")

    print("LANDING CSV :", out_csv)
    print("LANDING JSON:", out_jsonl)
    print(
        f"DT: {dt}  BATCH: {batch_id}  CSV_ROWS: {len(df)}  "
        f"FRAUD_RATE: {df['class'].mean():.4f}  EVENTS: {n_events}"
    )
    print(
        f"FAULT_INJECTION -> CSV bad rows injected: {injected_csv} "
        f"(BAD_RATE_CSV={bad_rate_csv}) | Events bad lines injected: {injected_events} "
        f"(BAD_RATE_EVENTS={bad_rate_events})"
    )


if __name__ == "__main__":
    main()
