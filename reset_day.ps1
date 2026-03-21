param(
  [Parameter(Mandatory=$true)]
  [string]$DT
)

# ==============================
# CONFIG
# ==============================
$ProjectRoot = "C:\Arturo\Consubanco\fraud_proyect"
$PythonExe   = Join-Path $ProjectRoot "venv\Scripts\python.exe"
$DbtExe      = Join-Path $ProjectRoot "venv\Scripts\dbt.exe"
$LogDir      = Join-Path $ProjectRoot "logs\reset"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$LogFile     = Join-Path $LogDir ("reset_{0}_{1}.log" -f $DT, (Get-Date).ToString("yyyyMMdd_HHmmss"))

# Postgres: usa PG_URL si existe; si no, default local
$PG_URL = $env:PG_URL
if ([string]::IsNullOrWhiteSpace($PG_URL)) {
  $PG_URL = "postgresql+psycopg2://fraud:fraudpass@localhost:5432/frauddb"
}

Set-Location $ProjectRoot
"=== RESET DAY START $(Get-Date -Format s) DT=$DT ===" | Out-File $LogFile -Append
"ProjectRoot=$ProjectRoot" | Out-File $LogFile -Append
"PG_URL=$PG_URL" | Out-File $LogFile -Append

# ==============================
# 1) FILESYSTEM RESET (landing/bronze/quarantine)
# ==============================
$paths = @(
  ".\data\landing\creditcard\dt=$DT",
  ".\data\landing\events\dt=$DT",
  ".\data\bronze\creditcard\dt=$DT",
  ".\data\bronze\events\dt=$DT",
  ".\data\quarantine\creditcard\dt=$DT",
  ".\data\quarantine\events\dt=$DT"
)

"--- Filesystem cleanup ---" | Out-File $LogFile -Append
foreach ($p in $paths) {
  try {
    if (Test-Path $p) {
      "Removing $p" | Out-File $LogFile -Append
      Remove-Item -Recurse -Force $p
    } else {
      "Skip (not found): $p" | Out-File $LogFile -Append
    }
  } catch {
    "ERROR removing $p : $($_.Exception.Message)" | Out-File $LogFile -Append
    throw
  }
}

# ==============================
# 2) DATABASE RESET (RAW by dt)
# ==============================
"--- Database cleanup (raw.* by dt) ---" | Out-File $LogFile -Append

# Ejecuta SQL de borrado usando SQLAlchemy via python (sin depender de psql)
$py = @"
import os
from sqlalchemy import create_engine, text

dt = os.environ["DT"]
pg_url = os.environ["PG_URL"]
engine = create_engine(pg_url, future=True)

sql = [
  "begin;",

  "delete from raw.creditcard_batches_quarantine where dt = :dt;",
  "delete from raw.creditcard_batches            where dt = :dt;",

  "delete from raw.transaction_events_quarantine where dt = :dt;",
  "delete from raw.transaction_events            where dt = :dt;",

  "delete from raw.batch_control                 where dt = :dt;",

  "commit;"
]

with engine.connect() as conn:
    for stmt in sql:
        conn.execute(text(stmt), {"dt": dt})
print("[OK] raw cleanup for dt=", dt)
"@

$env:DT = $DT
$env:PG_URL = $PG_URL

& $PythonExe -c $py 2>&1 | Out-File $LogFile -Append
if ($LASTEXITCODE -ne 0) {
  "ERROR: DB cleanup failed. EXITCODE=$LASTEXITCODE" | Out-File $LogFile -Append
  exit $LASTEXITCODE
}

# ==============================
# 3) REBUILD ANALYTICS (dbt tables)
# ==============================
"--- dbt rebuild ---" | Out-File $LogFile -Append
if (!(Test-Path $DbtExe)) {
  "ERROR: dbt.exe not found at $DbtExe" | Out-File $LogFile -Append
  exit 1
}

Push-Location (Join-Path $ProjectRoot "fraud_dbt")
& $DbtExe deps 2>&1 | Out-File $LogFile -Append
if ($LASTEXITCODE -ne 0) { Pop-Location; exit $LASTEXITCODE }

& $DbtExe build 2>&1 | Out-File $LogFile -Append
$exitCode = $LASTEXITCODE
Pop-Location

"=== RESET DAY END $(Get-Date -Format s) EXITCODE=$exitCode ===" | Out-File $LogFile -Append
exit $exitCode
