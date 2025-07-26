from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

dag = DAG(
    dag_id="customer360_etl",
    description="Customer 360 full ETL using Jupyter notebooks",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["customer360", "jupyter", "etl"],
)

with dag:
    extract = BashOperator(
        task_id="extract_csvs",
        bash_command="papermill /home/jovyan/work/etl/extract.ipynb /home/jovyan/work/etl/out_extract.ipynb"
    )

    transform = BashOperator(
        task_id="transform_with_spark",
        bash_command="papermill /home/jovyan/work/etl/transform.ipynb /home/jovyan/work/etl/out_transform.ipynb"
    )

    load = BashOperator(
        task_id="load_to_snowflake",
        bash_command="papermill /home/jovyan/work/etl/load_to_snowflake.ipynb /home/jovyan/work/etl/out_load.ipynb"
    )

    extract >> transform >> load
