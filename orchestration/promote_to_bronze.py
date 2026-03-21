import os
import shutil
from pathlib import Path
from datetime import datetime

# PROJECT_ROOT: por default, carpeta del repo; puedes override con env PROJECT_ROOT
PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT") or Path(__file__).resolve().parents[1])


def _latest_file(folder: Path, pattern: str) -> Path:
    files = sorted(folder.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise RuntimeError(f"No hay archivos {pattern} en {folder}")
    return files[0]


def _safe_move(src: Path, dst: Path) -> Path:
    """Idempotente: si dst ya existe, NO lo pisa; si src no existe, no hace nada."""
    if not src.exists():
        return dst
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        # ya promovido en una corrida previa
        return dst
    shutil.move(str(src), str(dst))
    return dst


def main():
    dt = os.environ.get("DT") or datetime.now().strftime("%Y-%m-%d")

    landing_cc_dir = PROJECT_ROOT / "data" / "landing" / "creditcard" / f"dt={dt}"
    landing_ev_dir = PROJECT_ROOT / "data" / "landing" / "events" / f"dt={dt}"

    bronze_cc_dir = PROJECT_ROOT / "data" / "bronze" / "creditcard" / f"dt={dt}"
    bronze_ev_dir = PROJECT_ROOT / "data" / "bronze" / "events" / f"dt={dt}"

    # Si no hay landing, no truena: permite re-ejecución completa sin bloquear
    if not landing_cc_dir.exists() and not landing_ev_dir.exists():
        print(f"[BRONZE] No existe landing para dt={dt}. Nada que promover. PROJECT_ROOT={PROJECT_ROOT}")
        return

    if landing_cc_dir.exists():
        src_csv = _latest_file(landing_cc_dir, "creditcard_*.csv")
        dst_csv = _safe_move(src_csv, bronze_cc_dir / src_csv.name)
        print("BRONZE CSV :", dst_csv)
    else:
        print(f"[BRONZE] landing creditcard no existe para dt={dt}: {landing_cc_dir}")

    if landing_ev_dir.exists():
        src_jsonl = _latest_file(landing_ev_dir, "events_*.jsonl")
        dst_jsonl = _safe_move(src_jsonl, bronze_ev_dir / src_jsonl.name)
        print("BRONZE JSON:", dst_jsonl)
    else:
        print(f"[BRONZE] landing events no existe para dt={dt}: {landing_ev_dir}")

    print("DT:", dt)


if __name__ == "__main__":
    main()
