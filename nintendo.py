from airflow import DAG
from datetime import datetime
import pendulum
from airflow.operators.python import PythonOperator
from nintendo.extract.extrair_magalu import get_data_magalu
from nintendo.extract.extrair_ml import get_data_ml
from nintendo.bronze.magalu import bronze_data_magalu
from nintendo.bronze.ml import bronze_data_ml
from nintendo.silver.magalu import silver_magalu
from nintendo.silver.ml import silver_ml



# Defina o fuso horário desejado (São Paulo, Brasil)
local_tz = pendulum.timezone('America/Sao_Paulo')

default_args = {
    "owner": "felipe.pegoraro",
    'email': ['felipepegoraro93@gmail.com'],
    'email_on_retry': False,
    'email_on_failure': True,
    "start_date": datetime(2024, 6, 24, tzinfo=local_tz),
}

dag = DAG(
    "nintendo",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    catchup=False
)

# EXTRACT STEP

magalu1 = PythonOperator(
    task_id='extract_magalu',
    python_callable=get_data_magalu,
    dag=dag
)

ml1 = PythonOperator(
    task_id='extract_ml',
    python_callable=get_data_ml,
    dag=dag
)

# BRONZE STEP

magalu2 = PythonOperator(
    task_id='bronze_magalu',
    python_callable=bronze_data_magalu,
    dag=dag
)

ml2 = PythonOperator(
    task_id='bronze_ml',
    python_callable=bronze_data_ml,
    dag=dag
)

# SELVER STEP

magalu3 = PythonOperator(
    task_id='silver_magalu',
    python_callable=silver_magalu,
    dag=dag
)

ml3 = PythonOperator(
    task_id='silver_ml',
    python_callable=silver_ml,
    dag=dag
)

[magalu1, ml1]

magalu1 >> magalu2
ml1 >> ml2

[magalu2, ml2]

magalu2 >> magalu3
ml2 >> ml3

[magalu3, ml3]