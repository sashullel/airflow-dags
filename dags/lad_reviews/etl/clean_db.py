import airflow
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import timedelta

DEFAULT_ARGS = {
    'owner': 'sashullel',
    'email': ['klen.sasha_22@list.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 20,
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG(
    dag_id='clean_db_dag',
    schedule_interval=None,
    catchup=False,
    tags=['etl'],
    default_args=DEFAULT_ARGS
    )


drop_tables = '''
DROP TABLE frequency;
DROP TABLE reviews;
DROP TABLE sources;
'''


drop_tables_task = PostgresOperator(
    dag=dag,
    task_id='drop_tables',
    postgres_conn_id='pg_connector',
    sql=drop_tables
    )

drop_tables_task