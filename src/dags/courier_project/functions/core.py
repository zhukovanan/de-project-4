import requests
import json
import pandas as pd
import psycopg2
import sys

from airflow.exceptions import AirflowException
from datetime import datetime

API_KEY = '25c27781-8fde-4b30-a22e-524044a7580f'
BASE_URL = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'


nickname = 'zhukov-an-an'
cohort = '1'
headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': API_KEY,
    'Content-Type': 'application/x-www-form-urlencoded'
}



def pg_connect(database='de', user='jovyan', password='jovyan', host='localhost', port= '5432'):
    try:
        server_conn = psycopg2.connect(
                    database=database, 
                    user=user, 
                    password=password, 
                    host=host, 
                    port= port
                    )
    except Exception as e:
        print(f"Not managed to connect specified server: {str(e)}")
    
    else:
        return server_conn



def get_core_report(report_type, sort_field, sort_direction, **kwargs ):

    print(f'Making request to get data on {report_type}')


    params = {'sort_field': sort_field,
              'sort_direction' : sort_direction,
              'limit': 50,
              'offset': 0}

    if report_type == 'deliveries':
        params = {'restaurant_id' : kwargs['restaurant_id'],
                       'from':kwargs['from_date'],
                       'to' : kwargs['to_date'],
                       'sort_field': sort_field,
                       'sort_direction' : sort_direction,
                       'limit': 50,
                       'offset': 0}

        if kwargs['remove_params_flag'] == True:
            params.pop('restaurant_id', None)
            params.pop('from', None)
            params.pop('to', None)

    offset = 1

    data_lst = []
    
    while True:

        response = requests.get(f'{BASE_URL}/{report_type}', headers=headers, params=params)
        response.raise_for_status()

        response = json.loads(response.content)

        if not response:
            break

        data_lst.append(response)

        offset += 50

        params.update({'offset' : offset})

        if len(response) < 50:
            break
    

    
    return  [x for y in data_lst for x in y]


def load_data_from_base(report_type, table, sort_field = '_id', sort_direction = 'asc', pg=pg_connect(), **context):

    load_time_id = context['execution_date']
    run_id = hash(context['task_instance_key_str'])

    with pg as de_conn:
        with de_conn.cursor() as cur:

            df = pd.DataFrame(get_core_report(report_type=report_type, sort_field=sort_field, sort_direction=sort_direction))

            df['upload_at'] = str(load_time_id)
            df['run_id'] =  run_id

            query = f"INSERT INTO stg.{table} (id_{report_type[:-1]}, name, upload_at, run_id) values %s"
            psycopg2.extras.execute_values(cur,query, df.values)

        de_conn.commit()
        cur.close()
    de_conn.close()



def load_deliveries_to_base(sort_field = '_id', sort_direction = 'asc',pg=pg_connect(), remove_params_flag = True, **context):

    ds_date = context['ds']
    from_date = datetime.strptime(ds_date, '%Y-%m-%d').replace(day=1)
    to_date =  from_date + pd.offsets.MonthEnd(1)


    load_time_id = context['execution_date']
    run_id = hash(context['task_instance_key_str'])


    with pg as de_conn:
        with de_conn.cursor() as cur:
            
            cur.execute("SELECT distinct id_restaurant from stg.restaurants ")
            records = cur.fetchall()

            rest_ids = [i[0] for i in records]
            for restaurant in rest_ids:
                df = pd.DataFrame(get_core_report(report_type='deliveries', remove_params_flag = remove_params_flag, sort_field=sort_field, sort_direction=sort_direction,
                                  restaurant_id=restaurant, from_date=from_date, to_date = to_date))

                df['restaurant_id'] = restaurant
                df['upload_at'] = str(load_time_id)
                df['run_id'] =  run_id

                query = f"INSERT INTO stg.deliveries (order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum, restaurant_id, upload_at, run_id) values %s"
                psycopg2.extras.execute_values(cur,query, df.values)

        de_conn.commit()
        cur.close()
    de_conn.close()





