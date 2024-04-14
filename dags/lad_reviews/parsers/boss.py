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
DATA_PATH = 'parsed/boss.csv'

DEFAULT_ARGS = {
    'owner': 'sashullel',
    'email': ['klen.sasha_22@list.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 20,
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG(
    dag_id='boss_dag',
    schedule_interval=None,
    catchup=False,
    tags=['parser'],
    default_args=DEFAULT_ARGS
    )


def parse_boss():
    s3_hook = S3Hook('s3')
    session = s3_hook.get_session(s3_hook.conn_config.region_name)
    resource = session.resource('s3', endpoint_url=s3_hook.conn_config.endpoint_url)
    csv_buf = StringIO()

    seed_url = 'https://boss-nadzor.ru/company/55644/?page='
    headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en,ru;q=0.9,ru-RU;q=0.8,en-US;q=0.7,de;q=0.6",
            "cache-control": "max-age=0",
            "sec-ch-ua": "\"Not_A Brand\";v=\"8\", \"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\"",
            "sec-ch-ua-mobile": ",?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
        }


    total_pages = 1
    encoding = 'utf-8'
    timeout = 2
    should_verify_certificate = True

    def make_request(url: str) -> requests.models.Response:
        time.sleep(random.randint(2, 4))
        response = requests.get(url,
                                headers=headers,
                                timeout=timeout,
                                verify=should_verify_certificate)
        response.raise_for_status()
        response.encoding = encoding
        return response

    def parse_review(soup) -> dict:
        date_str = soup.find('div', {'class': 'review__date'}).text.strip()
        months = {'Января': 'January', 'Февраля': 'February', 'Марта': 'March', 'Апреля': 'April',
                'Мая': 'May', 'Июня': 'June', 'Июля': 'July', 'Августа': 'August',
                'Сентября': 'September', 'Октября': 'October', 'Ноября': 'November', 'Декабря': 'December'}

        day, month_str, year = date_str.split()
        month = months[month_str.capitalize()]
        date_str = f'{day} {month} {year}'
        date = datetime.strptime(date_str, '%d %B %Y')

        ratings_elements = soup.find_all('div', {'class': 'dj-rating__item'})
        ratings = []

        for rate in ratings_elements:
            rate_elements = rate.find_all('svg', {'aria-label': 'star fill'})
            rate = float(len(rate_elements))
            ratings.append(rate)

        review_rating = np.round(np.nanmean(ratings), 1)

        city = soup.find('div', {'class': 'tags__item'}).text
        review = soup.find('div', {'class': 'review__text'}).text.split('Читать далее...')[0].strip()
        link = url

        review_info = {'Дата': date, 'Источник': 'Босс-надзор', 'Ссылка': link, 'Город': city,
                        'Отзыв': review, 'Достоинства': None, 'Недостатки': None, 'Ответ': None,
                        'Оценка': review_rating, 'Условия труда': ratings[0],
                        'Уровень дохода': ratings[1], 'Коллектив': ratings[2], 'Руководство': ratings[3],
                        'Условия для отдыха': ratings[4], 'Возможности роста': ratings[5]}
        return review_info


    def extract_page_reviews(response: requests.models.Response) -> list:
        page_soup = BeautifulSoup(response.text, 'lxml')
        reviews = page_soup.find_all('div', {'class': 'review'})
        page_reviews_list = [parse_review(review) for review in reviews]
        return page_reviews_list

    urls = [f'{seed_url}{i + 1}' for i in range(total_pages)]
    all_reviews_list = []

    for url in urls:
        response = make_request(url)
        all_reviews_list.extend(extract_page_reviews(response))

    df = pd.DataFrame(all_reviews_list)
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)

    resource.Object(BUCKET, DATA_PATH).put(Body=csv_buf.getvalue())
    