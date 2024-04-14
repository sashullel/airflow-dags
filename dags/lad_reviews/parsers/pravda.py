import airflow
from airflow.models import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import timedelta
from io import StringIO

from bs4 import BeautifulSoup
import lxml
import datetime
import random
import re
import requests
import time
import numpy as np
import pandas as pd

BUCKET = 'sashullel-airflow-logs'
DATA_PATH = 'parsed/pravda.csv'

DEFAULT_ARGS = {
    'owner': 'sashullel',
    'email': ['klen.sasha_22@list.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 20,
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG(
    dag_id='pravda_dag',
    schedule_interval=None,
    catchup=False,
    tags=['parser'],
    default_args=DEFAULT_ARGS
    )

def parse_pravda():
    s3_hook = S3Hook('s3')
    session = s3_hook.get_session(s3_hook.conn_config.region_name)
    resource = session.resource('s3', endpoint_url=s3_hook.conn_config.endpoint_url)
    csv_buf = StringIO()
    
    seed_url = 'https://pravda-sotrudnikov.ru/company/lad-4?page='
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
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
    
    total_pages = 1
    encoding = 'utf-8'
    timeout = 5
    should_verify_certificate = True


    def make_request(url: str) -> requests.models.Response:
        time.sleep(random.randint(3, 10))
        response = requests.get(url,
                                headers=headers,
                                timeout=timeout,
                                verify=should_verify_certificate)
        response.raise_for_status()
        response.encoding = encoding
        return response

    def parse_review(review_soup) -> dict:
        date_str = review_soup.find('div', {'class': 'company-reviews-list-item-date'}).text.strip()
        date = datetime.datetime.strptime(date_str, '%H:%M %d.%m.%Y')

        source = 'Правда сотрудников'

        link = 'https://pravda-sotrudnikov.ru' + review_soup.find('a', {'class': 'btn btn-yellow show-answers-button'}).get('href')

        almost_city = review_soup.find('div', {'class': 'company-reviews-list-item-city'}).text.strip()
        city = re.search(r'(?<=Город: )[^.]*', almost_city).group(0)

        pros_cons = review_soup.find_all('div', {'class': 'company-reviews-list-item-text-message'})
        pros, cons = pros_cons[0].text.strip(), pros_cons[1].text.strip()

        tbl = str.maketrans({':': None})

        labels = review_soup.find_all('span', {'company-reviews-list-item-ratings-item-label'})
        values = review_soup.find_all('span', {'class': 'company-reviews-list-item-ratings-item-stars rating-autostars'})
        ratings = {l.text.strip().translate(tbl): float(v.attrs['data-rating']) for l, v in zip(labels, values)}
        overall = np.round(np.nanmean(list(ratings.values())), 1)

        review_info = {'Дата': date.date(), 'Источник': source, 'Ссылка': link, 'Город': city,
                        'Отзыв': None, 'Достоинства': pros, 'Недостатки': cons, 'Ответ': None,
                        'Оценка': overall}
        
        return {**review_info, **ratings}


    def extract_page_reviews(response: requests.models.Response) -> list:
        page_soup = BeautifulSoup(response.text, 'lxml')
        reviews = page_soup.find_all('div', {'class': 'company-reviews-list-item'})
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
    