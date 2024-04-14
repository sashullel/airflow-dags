import airflow
from airflow.models import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import psycopg2

BUCKET = 'sashullel-airflow-logs'

DEFAULT_ARGS = {
    'owner': 'sashullel',
    'email': ['klen.sasha_22@list.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 20,
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG(
    dag_id='make_db_dag',
    schedule_interval=None,
    catchup=False,
    tags=['etl'],
    default_args=DEFAULT_ARGS
    )


create_tables = '''
CREATE TABLE IF NOT EXISTS frequency (id INTEGER NOT NULL,
review VARCHAR,
advantages VARCHAR,
disadvantages VARCHAR,
PRIMARY KEY(id));

CREATE TABLE IF NOT EXISTS reviews (id INTEGER NOT NULL, 
date DATE,
source INTEGER,
link VARCHAR,
city VARCHAR,
coordinates VARCHAR,
review TEXT,
advantages TEXT,
disadvantages TEXT,
answer TEXT,
grade FLOAT,
working_conditions FLOAT, 
team FLOAT, 
management FLOAT,
recreation_conditions FLOAT,
employee_benefits FLOAT,
career_growth FLOAT,
salary FLOAT, 
others FLOAT,
PRIMARY KEY(id));

CREATE TABLE IF NOT EXISTS sources (id INTEGER NOT NULL,
source VARCHAR,
PRIMARY KEY(id));
'''

clean_tables = '''
DELETE FROM frequency;
DELETE FROM reviews;
DELETE FROM sources;
'''


def connect():
    s3_hook = S3Hook('s3')
    session = s3_hook.get_session(s3_hook.conn_config.region_name)
    resource = session.resource('s3', endpoint_url=s3_hook.conn_config.endpoint_url)
    objects_lst = s3_hook.get_bucket(BUCKET).objects.all()
    tables = [obj.get()['Body'] for obj in objects_lst if obj.key.startswith('final_tables/')]

    pg_hook = PostgresHook(postgres_conn_id="pg_connector")
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    table_names = ['frequency', 'reviews', 'sources']
    for table, name in zip(tables, table_names):
        cur.copy_expert(f"COPY {name}  FROM STDIN WITH (FORMAT CSV, DELIMITER ',');", table)
        conn.commit()
        