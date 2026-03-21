import subprocess

def run(cmd):
    print(f"\n=== Running: {cmd} ===\n")
    subprocess.run(cmd, shell=True, check=True)

if __name__ == "__main__":
    
    # 1. Inicializar control de batches
    run("python init_batch_control.py")
    
    # 2. Ejecutar ingesta diaria
    run("python run_daily.py")
    
    # 3. Ejecutar dbt
    run("dbt deps --project-dir fraud_dbt")
    run("dbt build --project-dir fraud_dbt")
    
    # 4. Entrenar modelo ML
    run("python ml/train_model.py")
    
    print("\n✅ PROTOTIPO COMPLETADO\n")