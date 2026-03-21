--PROCESAR UN DIA SIN GENERAR ARCHIVOS NUEVOS (SOLO LANNDING)

cd C:\Arturo\Consubanco\fraud_proyect
venv\Scripts\activate

$env:DT="2026-02-10"
$env:SIMULATE="0"
python run_daily.py

cd fraud_dbt
dbt build
cd ..

------------

RESET UN DIA

cd C:\Arturo\Consubanco\fraud_proyect
.\reset_day.ps1 -DT "2026-02-10"


----

SIMULACION

cd C:\Arturo\Consubanco\fraud_proyect
$env:DT="2026-02-12"
python simulate_arrival.py
Remove-Item Env:\DT


-----INICIO AMBIENTES

docker compose up -d

Arrancar Prefect
T1
cd C:\Arturo\Consubanco\fraud_proyect
venv\Scripts\activate
prefect server start

T2
cd C:\Arturo\Consubanco\fraud_proyect
venv\Scripts\activate
prefect worker start --pool fraud-pool

T3
cd C:\Arturo\Consubanco\fraud_proyect
venv\Scripts\activate
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"


UI Prefect:
http://127.0.0.1:4200

MetaBase
http://localhost:3000/

prefect worker ls


Schedule existente:

prefect deployment ls
-------------------------------------------------
prefect deployment run "fraud-daily-pipeline/fraud-daily"
prefect deployment run "fraud-daily-pipeline/fraud-daily" --params '{"simulate":"1"}'

