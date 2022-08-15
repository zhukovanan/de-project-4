import requests
import json
import pandas as pd
import psycopg2
import logging

from airflow.models import Connection
from datetime import datetime

#Для логирования
task_logger = logging.getLogger('airflow.task')

#Забираем параметры подключения к yandexcloud
CONN_CLOUD = Connection.get_connection_from_secrets("CLOUD_CONNECTION_KEY")
CONN_PG = Connection.get_connection_from_secrets("PG_WAREHOUSE_CONNECTION")

API_KEY = CONN_CLOUD.password
BASE_URL = CONN_CLOUD.host


nickname = CONN_CLOUD.login
cohort = '1'
headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': API_KEY,
    'Content-Type': 'application/x-www-form-urlencoded'
}


#Клиент для подключеня к БД
def pg_connect(database=CONN_PG.schema, user=CONN_PG.login, password=CONN_PG.password, host=CONN_PG.host, port = CONN_PG.port):
    try:
        server_conn = psycopg2.connect(
                    database=database, 
                    user=user, 
                    password=password, 
                    host=host, 
                    port= port
                    )
    except Exception as e:
        task_logger.error(f"Not managed to connect specified server: {str(e)}")    
    else:
        return server_conn


#Получаем информацию по разным типам структурных элементов данных
def get_core_report(report_type, sort_field, sort_direction, **kwargs ):

    task_logger.info(f'Making request to get data on {report_type}')

    #Параметры запроса
    params = {'sort_field': sort_field,
              'sort_direction' : sort_direction,
              'limit': 50,
              'offset': 0}

    #Если тип забираем данные по доставкам, необходимо скорректировать структуру параметров
    if report_type == 'deliveries':
        params = {'restaurant_id' : kwargs['restaurant_id'],
                       'from':kwargs['from_date'],
                       'to' : kwargs['to_date'],
                       'sort_field': sort_field,
                       'sort_direction' : sort_direction,
                       'limit': 50,
                       'offset': 0}

        #Если включен параметр, то грузим все данные
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

            task_logger.error(f'Not managed to load information on {report_type}')

            break

        data_lst.append(response)

        offset += 50

        params.update({'offset' : offset})

        if len(response) < 50:

            task_logger.info(f'Taken all information on {report_type}')

            break
    

    
    return  [x for y in data_lst for x in y]

#Прогружаем данные в базу по ресторанам и курьерам
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


#Прогружаем данные в базу по доставкам
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





