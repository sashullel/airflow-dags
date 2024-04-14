import airflow
from airflow.models import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import timedelta
from io import StringIO
import csv


BUCKET = 'sashullel-airflow-logs'

DEFAULT_ARGS = {
    'owner': 'sashullel',
    'email': ['klen.sasha_22@list.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 20,
    'retry_delay': timedelta(minutes=1)
    }

with DAG(
    dag_id='final_tables_dag',
    schedule_interval=None,
    catchup=False,
    tags=['etl'],
    default_args=DEFAULT_ARGS
):


    def make_final_tables():
        from bs4 import BeautifulSoup
        import lxml
        from datetime import datetime
        import random
        import re
        import requests
        import time
        import numpy as np
        import pandas as pd

        import nltk
        nltk.download('stopwords')
        nltk.download('punkt')
        nltk.download('averaged_perceptron_tagger')
        from nltk import word_tokenize
        from nltk.corpus import stopwords
        from nltk.stem import WordNetLemmatizer
        from nltk.stem import SnowballStemmer

        import string
        import pymorphy2
        import collections
        from ctypes import sizeof
        from geopy.geocoders import Nominatim

        s3_hook = S3Hook('s3')
        session = s3_hook.get_session(s3_hook.conn_config.region_name)
        resource = session.resource('s3', endpoint_url=s3_hook.conn_config.endpoint_url)
        objects_lst = s3_hook.get_bucket(BUCKET).objects.all()
        objects =  [StringIO(obj.get()['Body'].read().decode('utf-8')) for obj in objects_lst if obj.key.startswith('parsed/')]
        dfs = [pd.read_csv(o) for o in objects]
        
        reviews_df = pd.concat(dfs)

        # sources and reviews tables
        sources = reviews_df['Источник'].unique()
        coded_sources = {sources[i]: i + 1 for i in range(len(sources))}

        reviews_df['Источник'] = reviews_df['Источник'].apply(lambda x: coded_sources[x])
        reviews_df.insert(0, 'id', range(1, len(reviews_df) + 1))

        sources_df = pd.DataFrame(list(zip(coded_sources.values(), coded_sources.keys())),
                            columns=['id', 'Источник'])


        # frequency table
        review_columns = ['Отзыв', 'Достоинства', 'Недостатки']
        almost_frequency_df = reviews_df[review_columns]
        top_n = 10
        data = {}

        for col in review_columns:
            text = ' '.join(almost_frequency_df[col].dropna(how='all').fillna('').tolist())
            lemmatizer = pymorphy2.MorphAnalyzer()
            current_stopwords = set(stopwords.words('russian'))
            new_stopwords = {'работа', 'недостаток', 'компания', 'мочь', 'человек', 'год', 'рабочий', 'сотрудник', 'возможность', 'утро', 'вечер'}
            current_stopwords.update(new_stopwords)
            clean_text = text.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
            tokens = nltk.word_tokenize(clean_text)
            lemmas = [lemmatizer.parse(t)[0].normal_form for t in tokens]
            lemmas = [l for l in lemmas if l not in current_stopwords]
            nouns = [l for l in lemmas if (('NOUN' in lemmatizer.parse(l)[0].tag) & (len(l) > 1))]
            word_count = collections.Counter(nouns)
            col_top_words = list(dict(word_count.most_common(top_n)).keys())
            data[col] = col_top_words

        frequency_df = pd.DataFrame(data)
        frequency_df.insert(0, 'id', range(1, len(frequency_df) + 1))


        # modifying columns of reviews table
        phrase_list = [['соц', 'пакет'], ['рост', 'перспектив'], ['з/п', 'зарплат', 'доход'], ['коллект', 'коллег'], ['услов', 'труд', 'работ'],
                    ['руководств', 'менедж'], ['услов', 'отд']]
        column_list = ['Соц. пакет', 'Карьерный рост', 'Зарплата', 'Коллектив', 'Условия труда', 'Руководство', 'Условия для отдыха', 'Другое']
        
        
        morph = pymorphy2.MorphAnalyzer()

        def lemmatize_text(text):
            tokens = word_tokenize(text)
            lemmatized_words = [morph.parse(token)[0].normal_form for token in tokens]

            stemmer = SnowballStemmer(language='russian')
            stem = [stemmer.stem(stem) for stem in lemmatized_words]
            return stem

        def merge_columns(column_name, add_column, df):
            if (df[add_column].dtype == float and add_column != 'Оценка') and (column_name != add_column or df.columns.tolist().count(add_column) > 1):
                df[column_name] = df.apply(lambda row: round((row[column_name] + row[add_column]) / 2, 1) if not pd.isnull(row[column_name]) and not pd.isnull(row[add_column]) else row[column_name] if not pd.isnull(row[column_name]) else row[add_column], axis=1)
                df = df.drop(add_column, axis=1, inplace=True)

        def check_columns(df):
            df_columns = df.columns.to_list()
            for column in column_list:
                if column not in df_columns:
                    df[column] = pd.Series([], dtype=float)
        
        def define_column(column_name, df):
            upd_column_name = lemmatize_text(column_name)
            i = 0
            ch = True
            for lst in phrase_list:
                result = [(word, phrase) for word in upd_column_name for phrase in lst if phrase in word]
                if len(result) >= 2 or (len(result) == 1 and i in range(1, 6) and i != 4):
                    merge_columns(column_list[i], column_name, df)
                    ch = False
                i += 1
            if ch:
                merge_columns(column_list[7], column_name, df)

        
        check_columns(reviews_df)
        column_names = reviews_df.columns.to_list()
        for column in column_names:
            define_column(column, reviews_df)
        

        # create a coordinates column
        geolocator = Nominatim(user_agent="klensashka.22@gmail.com")

        def get_coordinates(city):
            if pd.isna(city):  
                return ''
            else:
                location = geolocator.geocode(city)
                if location:
                    return [location.latitude, location.longitude]
                else:
                    return ''

        reviews_df['Координаты'] = reviews_df['Город'].apply(get_coordinates)
        moving_column = ['Координаты']
        index = reviews_df.columns.get_loc('Город') + 1
        index_end = reviews_df.columns.get_loc('Координаты')
        reviews_df = reviews_df[list(reviews_df.columns[:index]) + moving_column + list(reviews_df.columns[index:index_end])]
        
        # saving csv files
        sources_csv_buf = StringIO()
        frequency_csv_buf = StringIO()
        reviews_csv_buf = StringIO()

        sources_df.to_csv(sources_csv_buf, header=False, index=False)
        sources_csv_buf.seek(0)
        resource.Object(BUCKET, 'final_tables/sources.csv').put(Body=sources_csv_buf.getvalue())

        reviews_df.to_csv(reviews_csv_buf, header=False, index=False)
        reviews_csv_buf.seek(0)
        resource.Object(BUCKET, 'final_tables/reviews.csv').put(Body=reviews_csv_buf.getvalue())

        frequency_df.to_csv(frequency_csv_buf, header=False, index=False)
        frequency_csv_buf.seek(0)
        resource.Object(BUCKET, 'final_tables/frequency.csv').put(Body=frequency_csv_buf.getvalue())
