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
DATA_PATH = 'parsed/dreamjob.csv'

DEFAULT_ARGS = {
    'owner': 'sashullel',
    'email': ['klen.sasha_22@list.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 20,
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG(
    dag_id='dreamjob_dag',
    schedule_interval=None,
    catchup=False,
    tags=['parser'],
    default_args=DEFAULT_ARGS
    )

def parse_dreamjob():
    from selenium import webdriver

    s3_hook = S3Hook('s3')
    session = s3_hook.get_session(s3_hook.conn_config.region_name)
    resource = session.resource('s3', endpoint_url=s3_hook.conn_config.endpoint_url)
    csv_buf = StringIO()

    seed_url = 'https://dreamjob.ru/employers/289753'
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en,ru;q=0.9,ru-RU;q=0.8,en-US;q=0.7,de;q=0.6",
        "cache-control": "max-age=0",
        "cookie": "symfony=94a44eaa8272f9af84162aa462a09edf; _ga=GA1.1.1840283532.1703661618; _ga_DVPZGC42G2=GS1.1.1704095917.3.0.1704095917.60.0.0",
        "sec-ch-ua": "\"Not_A Brand\";v=\"8\", \"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
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

 
    def extract_page_reviews(response: requests.models.Response) -> list:
        options = webdriver.ChromeOptions()
        options.add_argument('--ignore-ssl-errors=yes')
        options.add_argument('--ignore-certificate-errors')

        user_agent = 'userMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
        options.add_argument(f'user-agent={user_agent}')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('-headless')

        remote_webdriver = 'remote_chromedriver'
        # remote_webdriver = 'http://0.0.0.0'
        # remote_webdriver1 = 'selenium'
        # remote_webdriver2 = 'selenium_selenium.1.a65ofcbumw3ic54crpdpw3unz'

        with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=options) as driver:
            driver.get(seed_url)
            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight - 1600);")
                time.sleep(6)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            
            page_soup = BeautifulSoup(driver.page_source, 'lxml')

        reviews = page_soup.find_all('div', {'class': 'review'})

        # define function for parsing one review
        def parse_review(review_soup) -> dict:
            months = {'Январь': '01', 'Февраль': '02', 'Март': '03', 'Апрель': '04', 'Май': '05', 'Июнь': '06', 'Июль': '07', 'Август': '08', 'Сентябрь': '09', 'Октябрь': '10', 'Ноябрь': '11', 'Декабрь': '12'}
            date_lst = review_soup.find('div', {'class': 'review__date'}).text.strip().split()
            month, year = months[date_lst[0]], date_lst[1]
            date = datetime.datetime.strptime(month + year, '%m%Y')

            source = 'Dream Job'

            link = review_soup.find('a', {'class': 'bt bt--32 bt--primary-link icon-copy'}).get('data-clipboard')

            tags = list(filter(None, [t.text.strip() for t in review_soup.find('div', {'class': 'tags'})]))
            city = tags[-1] if ('Стаж' not in tags[-1] and 'сотрудник' not in tags[-1]) else None
  
            pros, cons = review_soup.find_all('div', {'class': 'review__text'})
            pros, cons = pros.text, cons.text

            labels = ['Условия труда', 'Уровень дохода', 'Коллектив', 'Руководство', 'Условия для отдыха', 'Возможности роста']
            values = review_soup.find('div', {'class': 'dj-rating'}).attrs['data-partly-switch'].split('|')[1].split(',')
            ratings = {l: float(v) for l, v in zip(labels, values)}
            overall = np.round(np.nanmean(list(ratings.values())), 1)

            link = review_soup.find('a', {'class': 'bt bt--32 bt--primary-link icon-copy'}).get('data-clipboard')

            possible_answer = review_soup.find('div', {'class': 'review__answer-msg'})
            answer = possible_answer.text.strip() if possible_answer else None

            review_info = {'Дата': date.date(), 'Источник': source, 'Ссылка': link, 'Город': city,
                        'Отзыв': None, 'Достоинства': pros, 'Недостатки': cons, 'Ответ': answer,
                        'Оценка': overall}
            
            return {**review_info, **ratings}

        # parse each review
        page_reviews_list = [parse_review(review) for review in reviews]
        return page_reviews_list


    all_reviews_list = []
    response = make_request(seed_url)
    all_reviews_list.extend(extract_page_reviews(response))
    
    df = pd.DataFrame(all_reviews_list)
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    
    resource.Object(BUCKET, DATA_PATH).put(Body=csv_buf.getvalue())
