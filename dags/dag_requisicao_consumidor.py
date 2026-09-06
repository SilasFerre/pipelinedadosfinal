from airflow import DAG
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.operators.python import PythonOperator


from include.common import default_args
from include.consumidor import processar_dados,simular_treinamento,validar_treinamento

with DAG(
    'dag_consumidora_externa',             
    default_args=default_args,
    schedule=None,
    catchup = False,
    description='Dag Consumidora',
    tags = ['final']
) as dag:

    # Aguarda a conclusão da execução correspondente da DAG produtora
    esperar_produtora = ExternalTaskSensor(
        task_id="esperar_produtora",
        external_dag_id="dag_produtora_externa",
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
    )

    # Recupera pelo XCom os caminhos das imagens e valida os arquivos recebidos
    preparar_imagens = PythonOperator(
        task_id="processar_imagens",
        python_callable=processar_dados
    )    

    # Simula o treinamento e retorna métricas para a próxima task via XCom.
    treinar_modelo = PythonOperator(
        task_id="treinar_modelo",
        python_callable=simular_treinamento,
    )

    # Valida se o treinamento simulado atingiu os critérios definidos.
    validar_modelo = PythonOperator(
        task_id="validar_modelo",
        python_callable=validar_treinamento,
    )    

# Define a ordem obrigatória de execução das tasks da consumidora.
esperar_produtora >> preparar_imagens >> treinar_modelo >> validar_modelo
