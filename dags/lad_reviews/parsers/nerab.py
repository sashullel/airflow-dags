import airflow
from airflow.models import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import timedelta
from io import StringIO

from bs4 import BeautifulSoup
import lxml
from datetime import datetime
import random
import requests
import time
import numpy as np
import pandas as pd

BUCKET = 'sashullel-airflow-logs'
DATA_PATH = 'parsed/nerab.csv'

DEFAULT_ARGS = {
    'owner': 'sashullel',
    'email': ['klen.sasha_22@list.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 20,
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG(
    dag_id='nerab_dag',
    schedule_interval=None,
    catchup=False,
    tags=['parser'],
    default_args=DEFAULT_ARGS
    )


def parse_nerab():
    s3_hook = S3Hook('s3')
    session = s3_hook.get_session(s3_hook.conn_config.region_name)
    resource = session.resource('s3', endpoint_url=s3_hook.conn_config.endpoint_url)
    csv_buf = StringIO()

    url = 'https://nerab.ru/reviews/it-kompaniya-lad-juof'
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        reviews_data = []

        links = soup.find_all('a', {'href': True, 'class': 'link-icon'})
        reviews = soup.find_all('div', class_='review-description')
        ratings = soup.find_all('output', {'aria-valuenow': True})
        dates = soup.find_all('span', class_='review-date my-1')
        names = soup.find_all('span', class_='mr-4')

        for review, rating, date, name, link in zip(reviews, ratings, dates, names, links):
            if 'lad' in link['href']:
                review_text_element = review.find('span')
                review_text = review_text_element.text.strip() if review_text_element else "No text found"
                rating_text_element = float(rating['aria-valuenow'])

                date_element = date.text.strip()
                date_text = date.text.strip().title()[:-3]
                months = {'Января': 'January', 'Февраля': 'February', 'Марта': 'March', 'Апреля': 'April',
                        'Мая': 'May', 'Июня': 'June', 'Июля': 'July', 'Августа': 'August',
                        'Сентября': 'September', 'Октября': 'October', 'Ноября': 'November', 'Декабря': 'December'}

                day, month_str, year = date_text.split()
                month = months[month_str]
                standard_date_str = f'{day} {month} {year}'
                standard_date = datetime.strptime(standard_date_str, '%d %B %Y')

                reviews_data.append({'Дата': standard_date,
                                    'Источник': 'Не работаю бесплатно',
                                    'Ссылка': 'nerab.ru' + link['href'],
                                    'Город': '',
                                    'Отзыв': review_text,
                                    'Достоинства': '',
                                    'Недостатки': '',
                                    'Ответ': '',
                                    'Оценка': rating_text_element})

        df = pd.DataFrame(reviews_data)
        df.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)

        
    resource.Object(BUCKET, DATA_PATH).put(Body=csv_buf.getvalue())
    