from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    # Seu nome
    "owner": "<NOME_RESPONSAVEL>", 
    "start_date": datetime(2026, 2, 27),
    "retries": 0,
    "retry_delay": timedelta(minutes=15)
}

docs = """
# DAG para ingestão de dados via Google Sheets
Extrai dados tabulares do AppSheet ou aba editável: https://docs.google.com/spreadsheets/d/<SPREADSHEET_ID>/edit
"""

with DAG(
    "dbt_dag_extracao_parametros",
    default_args=default_args,
    schedule_interval="7/10 7-19 * * 1-5", 
    catchup=False,
    tags=["<TAG_AREA>", "<MATRICULA>", "<NOME_PROJETO>"], 
    doc_md=docs,
    max_active_runs=1
) as dag:
    
    inicio = EmptyOperator(task_id="inicio")
    fim = EmptyOperator(task_id="fim")

    task_1 = KubernetesPodOperator(
        task_id="dbt_step_1",
        name="pod_extracao_sheets",
        image="<AWS_ACCOUNT_ID>.dkr.ecr.<AWS_REGION>.amazonaws.com/<NOME_IMAGEM>:latest",
        cmds=["python3"],
        arguments=["/usr/datamart/<NOME_SCRIPT>.py"],
        namespace="processing",        
        is_delete_operator_pod=True,
        image_pull_policy="Always", 
        in_cluster=True
    )

    inicio >> task_1 >> fim