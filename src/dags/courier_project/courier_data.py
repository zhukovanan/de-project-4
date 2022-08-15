from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from courier_project.functions.core import load_data_from_base, load_deliveries_to_base

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

PG_DEFAULT = "PG_WAREHOUSE_CONNECTION"

files = ['couriers', 'restaurants', 'deliveries']

with DAG(
        'Courier_project_dag',
        default_args=args,
        description='Dag in response of get courier data stat',
        catchup=False,
        start_date=datetime.today() - timedelta(days=8),
        end_date=datetime.today() - timedelta(days=1),
) as dag:

    sql_creation_mart = []
    operators_to_get_files = []
    sql_creation_mart_dds = []
    sql_upload_dds_dm = []

    for file in files:
        sql_creation_mart.append(PostgresOperator(task_id=f'create_staging_{file}',
        postgres_conn_id=PG_DEFAULT,
        sql=f"sql/stg.{file}.sql"
    ))

    for file in files:
        if file == 'deliveries':
            type_dds = 'fct'
        else:
            type_dds = 'dm'
        sql_creation_mart_dds.append(PostgresOperator(task_id=f'create_{type_dds}_{file}',
        postgres_conn_id=PG_DEFAULT,
        sql=f"sql/dds.{type_dds}_{file}_build.sql"
    ))

    for file in files[:-1]:
        operators_to_get_files.append(PythonOperator(
        task_id=f'get_core_report_{file}',
        python_callable=load_data_from_base,
        op_kwargs={'report_type': f'{file}','table': f'{file}'}))

    for file in files[:-1]:
        sql_upload_dds_dm.append(PostgresOperator(task_id=f'upload_dds_dm_{file}',
        postgres_conn_id=PG_DEFAULT,
        sql=f"sql/dds.dm_{file}_upload.sql"))


    load_deliveries_to_db = PythonOperator(
        task_id=f'load_deliveries_to_db',
        python_callable=load_deliveries_to_base)

    upload_dds_fct_deliveries = PostgresOperator(task_id=f'upload_dds_fct_deliveries',
                                                 postgres_conn_id=PG_DEFAULT,
                                                sql=f"sql/dds.fct_deliveries_upload.sql")


    upload_cdm_dm_courier_ledger = PostgresOperator(task_id=f'upload_cdm_dm_courier_ledger',
                                                 postgres_conn_id=PG_DEFAULT,
                                                sql=f"sql/cdm.dm_courier_ledger.sql")

    i = 0   # iterator
    while i < len(sql_creation_mart)-1:
        (sql_creation_mart[i]
        >> sql_creation_mart_dds[i]
        >> sql_creation_mart[2]
        >> sql_creation_mart_dds[2]
        >> operators_to_get_files[i]
        >> load_deliveries_to_db
        >> sql_upload_dds_dm[i]
        >> upload_dds_fct_deliveries
        >> upload_cdm_dm_courier_ledger)
        i += 1
    


    





