import airflow
from airflow.models import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import timedelta
from io import StringIO

from bs4 import BeautifulSoup
import lxml
import datetime
import random
import requests
import time
import numpy as np
import pandas as pd

BUCKET = 'sashullel-airflow-logs'
DATA_PATH = 'parsed/orabote.csv'

DEFAULT_ARGS = {
    'owner': 'sashullel',
    'email': ['klen.sasha_22@list.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 20,
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG(
    dag_id='orabote_dag',
    schedule_interval=None,
    catchup=False,
    tags=['parser'],
    default_args=DEFAULT_ARGS
    )

def parse_orabote():
    s3_hook = S3Hook('s3')
    session = s3_hook.get_session(s3_hook.conn_config.region_name)
    resource = session.resource('s3', endpoint_url=s3_hook.conn_config.endpoint_url)
    csv_buf = StringIO()

    seed_url = 'https://orabote.day/feedback/list/company/58201/page/'
    headers = None

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

    def parse_review(review_soup) -> dict:
        author_info = review_soup.find('h3', {'class': 'stitle stitle--feedback'}).text.split()
        date = datetime.datetime.strptime(author_info[0], '%d.%m.%y')
        city = ' '.join(author_info[5:])

        full_review_link = review_soup.find('div', {'class': 'read-more-new'})['content']
        full_review = BeautifulSoup(make_request(full_review_link).text, 'lxml')
        boxes = full_review.find_all('div', {'class': 'news'})
        pros, cons = boxes[0].find('p').text.strip(), boxes[1].find('p').text.strip()

        if len(boxes) > 3:
            ratings = boxes[2].find('div', {'class': 'rating'}).find_all('div')
            ratings = [l.text.split('Рейтинг') for l in ratings]
            ratings_dict = {pair[0].strip(): float(pair[1][1]) for pair in ratings}
        else:
            ratings_dict = {}
        overall = np.round(np.nanmean(list(ratings_dict.values())), 1)

        review_info = {'Дата': date.date(), 'Источник': 'О Работе', 'Ссылка': full_review_link, 'Город': city,
                        'Отзыв': None, 'Достоинства': pros, 'Недостатки': cons, 'Ответ': None,
                        'Оценка': overall}
        
        return {**review_info, **ratings_dict}


    def extract_page_reviews(response: requests.models.Response) -> list:
        page_soup = BeautifulSoup(response.text, 'lxml')
        reviews = page_soup.find_all('div', {'class': 'news'})[:-2]
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
    