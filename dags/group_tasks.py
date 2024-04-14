from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from lad_reviews.parsers.boss import parse_boss
from lad_reviews.parsers.dreamjob import parse_dreamjob
from lad_reviews.parsers.habr import parse_habr
from lad_reviews.parsers.nerab import parse_nerab
from lad_reviews.parsers.orabote import parse_orabote
from lad_reviews.parsers.pravda import parse_pravda

from lad_reviews.etl.final_tables import make_final_tables
from lad_reviews.etl.make_db import create_tables, clean_tables, connect

DEFAULT_ARGS = {
    'owner': 'sashullel',
    'email': ['klen.sasha_22@list.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 20,
    'retry_delay': timedelta(minutes=1)
    }

with DAG('group_tasks', schedule_interval='@daily', start_date=days_ago(1), catchup=False, default_args=DEFAULT_ARGS) as dag:

    with TaskGroup('parsing') as parsing:
        boss_task = PythonOperator(
            task_id='parse_boss',
            python_callable=parse_boss,
            )
        dreamjob_task = PythonOperator(
            task_id='parse_dreamjob',
            python_callable=parse_dreamjob,
            )
        habr_task = PythonOperator(
            task_id='parse_habr',
            python_callable=parse_habr,
            )
        nerab_task = PythonOperator(
            task_id='parse_nerab',
            python_callable=parse_nerab,
        )
        orabote_task = PythonOperator(
            task_id='parse_orabote',
            python_callable=parse_orabote,
            )
        pravda_task = PythonOperator(
            task_id='parse_pravda',
            python_callable=parse_pravda,
            )   

    make_final_tables_task = PythonOperator(
        task_id='make_final_tables',
        python_callable=make_final_tables,
    )    
      
    with TaskGroup('database') as database:
        create_tables_task = PostgresOperator(
            dag=dag,
            task_id='create_tables',
            postgres_conn_id='pg_connector',
            sql=create_tables
            )

        clean_tables_task = PostgresOperator(
            dag=dag,
            task_id='clean_tables',
            postgres_conn_id='pg_connector',
            sql=clean_tables
            )

        connect_task = PythonOperator(
            task_id='connect',
            python_callable=connect,
            dag=dag
            )

        create_tables_task >> clean_tables_task >> connect_task

    parsing >> make_final_tables_task >> database
