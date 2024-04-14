import airflow
from airflow.models import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import timedelta
from io import StringIO

from bs4 import BeautifulSoup
import lxml
from datetime import datetime
import random
import re
import requests
import time
import numpy as np
import pandas as pd

BUCKET = 'sashullel-airflow-logs'
DATA_PATH = 'parsed/habr.csv'

DEFAULT_ARGS = {
    'owner': 'sashullel',
    'email': ['klen.sasha_22@list.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 20,
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG(
    dag_id='habr_dag',
    schedule_interval=None,
    catchup=False,
    tags=['parser'],
    default_args=DEFAULT_ARGS
    )


def parse_habr():
    s3_hook = S3Hook('s3')
    session = s3_hook.get_session(s3_hook.conn_config.region_name)
    resource = session.resource('s3', endpoint_url=s3_hook.conn_config.endpoint_url)
    csv_buf = StringIO()

    urls = ['https://career.habr.com/companies/lad/scores?page=1&sort=comment&year=2021',
        'https://career.habr.com/companies/lad/scores?page=2&sort=comment&year=2021',
        'https://career.habr.com/companies/lad/scores?page=3&sort=comment&year=2021']

    reviews_data = []
    all_ratings = []

    for url in urls:
        response = requests.get(url)

        if response.status_code == 200:
            print(url)

            soup = BeautifulSoup(response.text, 'html.parser')

            reviews = soup.find_all('div', class_='info users-about-items')
            dates = soup.find_all('div', class_='meta')
            scores = soup.find_all('div', class_='score_details')

            ratings_list = []

            for review, date, category in zip(reviews, dates, scores):

                gen = ''
                pros = ''
                cons = ''
                fixed_pros = ''
                fixed_cons = ''

                review_text_element = review.find_all('div', class_='row_info data')

                if len(review_text_element) == 1 and ('Достоинства' not in review_text_element[0].text.strip()
                                                    and 'Недостатки' not in review_text_element[0].text.strip()):
                    gen = review_text_element[0].text.strip()
                    pros = ''
                    cons = ''
                elif len(review_text_element) == 2:
                    gen = ''
                    pros = review_text_element[0].text
                    fixed_pros = re.sub(r'(?<=[а-я])(?=[А-Я])', ' ', pros[11:])
                    cons = review_text_element[1].text
                    fixed_cons = re.sub(r'(?<=[а-я])(?=[А-Я])', ' ', cons[10:])
                elif len(review_text_element) == 3:
                    gen = review_text_element[0].text.strip()
                    pros = review_text_element[1].text.strip()
                    fixed_pros = re.sub(r'(?<=[а-я])(?=[А-Я])', ' ', pros[11:])
                    cons = review_text_element[2].text.strip()
                    fixed_cons = re.sub(r'(?<=[а-я])(?=[А-Я])', ' ', cons[10:])

                date_text = date.text.strip().split('.')[0].title()
                months = {'Января': 'January', 'Февраля': 'February', 'Марта': 'March', 'Апреля': 'April',
                        'Мая': 'May', 'Июня': 'June', 'Июля': 'July', 'Августа': 'August',
                        'Сентября': 'September', 'Октября': 'October', 'Ноября': 'November', 'Декабря': 'December'}

                day, month_str, year = date_text.split()
                month = months[month_str]
                standard_date_str = f'{day} {month} {year}'
                standard_date = datetime.strptime(standard_date_str, '%d %B %Y')

                category_data = {'Дата': standard_date,
                                'Источник': 'Хабр Карьера',
                                'Ссылка': url,
                                'Город': '',
                                'Отзыв': gen,
                                'Достоинства': fixed_pros,
                                'Недостатки': fixed_cons,
                                'Ответ': ''}

                for cat in category:
                    category_title_element = cat.find('div', class_='title')
                    category_title = category_title_element.text.strip() if category_title_element else "No text found"
                    category_value_element = cat.find('div', class_='value')
                    category_value = category_value_element.text.strip() if category_value_element else "No text found"
                    category_data[category_title] = float(category_value)
                reviews_data.append(category_data)

    df = pd.DataFrame(reviews_data)

    numeric_columns = df.select_dtypes(include='float')
    row_means = numeric_columns.mean(axis=1).round(1)
    df.insert(8, "Оценка", row_means, False)
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)

    resource.Object(BUCKET, DATA_PATH).put(Body=csv_buf.getvalue())
    