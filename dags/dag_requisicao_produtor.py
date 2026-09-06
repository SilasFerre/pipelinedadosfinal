from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from include.common import default_args
from include.produtor import requisicao_foto

        
with DAG(
    'dag_produtora_externa',
    default_args=default_args,
    schedule=None,
    catchup = False,
    description='Dag Produtora',
    tags = ['final']
) as dag:

    # Baixa as imagens, salva no volume compartilhado e publica os caminhos via XCom.
    requisicao_fotos = PythonOperator(
        task_id='requisicao_fotos',
        python_callable=requisicao_foto
    )    

    # Dispara a consumidora usando os mesmos run_id e logical_date da produtora.
    disparar_consumidora = TriggerDagRunOperator(
        task_id="disparar_consumidora",
        trigger_dag_id="dag_consumidora_externa",
        trigger_run_id="{{ run_id }}",
        logical_date="{{ logical_date }}",
        wait_for_completion=False,
        skip_when_already_exists=True,
    )    

# A consumidora só é disparada após o download das imagens terminar com sucesso.
requisicao_fotos  >> disparar_consumidora
