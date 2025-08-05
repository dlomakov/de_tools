import requests
from datetime import datetime
import logging
import pandas as pd

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
    'email': ['denis_lomakov@mail.ru'],
    'poke_interval': 600
}

with DAG("yarn_api_parser",
    description='Load data about apps resources consumption from API YARN to PostgreSQL',
    schedule_interval='@daily',
    default_args=default_args,
    max_active_runs=1,
    tags=['d-lomakov']
) as dag:
    
    def load_yarn_data_to_postgres():
        yarn_url = 'http://32edb4956e4d.vps.myjino.ru:49494/ws/v1/cluster/apps'
        response = requests.get(yarn_url)

        if response.status_code == 200:
            logging.info("success http request YARN API")
            data = response.json()

            apps_data = []

            for app in data['apps']['app']:
                app_info = {
                    'app_id': app['id'],
                    'app_name': app['name'],
                    'cluster_id': app['clusterId'],
                    'queue_name': app['queue'],
                    'user_name': app['user'],
                    'app_type': app['applicationType'],
                    'app_state': app['state'],
                    'app_finalstatus': app['finalStatus'],
                    'app_memoryseconds': app['memorySeconds'],
                    'app_vcoreseconds': app['vcoreSeconds'],
                    'app_startedtime': pd.to_datetime(app['startedTime'],unit='ms'),
                    'app_launchtime': pd.to_datetime(app['launchTime'],unit='ms'),
                    'app_finishedtime': pd.to_datetime(app['finishedTime'],unit='ms'),
                    'app_elapsedtime': app['elapsedTime'],
                    'airflow_datechange': datetime.now()
                }

                apps_data.append(app_info)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load from YARN API')

        insert_values = [
            f"('{app_data['app_id']}', '{app_data['app_name']}', '{app_data['cluster_id']}', '{app_data['queue_name']}', '{app_data['user_name']}', '{app_data['app_type']}', '{app_data['app_state']}', '{app_data['app_finalstatus']}', {app_data['app_memoryseconds']}, {app_data['app_vcoreseconds']}, '{app_data['app_startedtime']}', '{app_data['app_launchtime']}', '{app_data['app_finishedtime']}', {app_data['app_elapsedtime']}, '{app_data['airflow_datechange']}')"
            for app_data in apps_data]

        table_name_stg = 's_app_resources_consumption'

        insert_sql = f"INSERT INTO {table_name_stg} VALUES {','.join(insert_values)}"
        pg_hook = PostgresHook(postgres_conn_id='datamart')
        pg_hook.run(insert_sql, True)
        logging.info('success insert into db')
        return insert_values

    load_yarn_data_to_postgres_task = PythonOperator(
        task_id='load_yarn_data_to_postgres',
        python_callable=load_yarn_data_to_postgres,
        provide_context=True,
        dag=dag
    )
    
    def filter_and_deduplicate():
        table_name = 't_app_resources_consumption'
        filter_and_dedupe_sql = f"""
            WITH filtered_apps AS (
                SELECT
                    sarc.app_id
                    ,sarc.app_name
                    ,sarc.cluster_id
                    ,sarc.queue_name
                    ,sarc.user_name
                    ,sarc.app_type
                    ,sarc.app_state
                    ,sarc.app_finalstatus
                    ,sarc.app_memoryseconds
                    ,sarc.app_vcoreseconds
                    ,sarc.app_startedtime
                    ,sarc.app_launchtime
                    ,sarc.app_finishedtime
                    ,sarc.app_elapsedtime
                    ,sarc.airflow_datechange
                    ,ROW_NUMBER() OVER (PARTITION BY sarc.app_id ORDER BY sarc.airflow_datechange DESC) as rn
                FROM
                    s_app_resources_consumption sarc
                WHERE
                    sarc.app_finalstatus = 'SUCCEEDED'
                    AND sarc.app_type = 'SPARK'
            )
            insert into {table_name}
            SELECT
                fa.app_id
                ,fa.app_name
                ,fa.cluster_id
                ,fa.queue_name
                ,fa.user_name
                ,fa.app_type
                ,fa.app_state
                ,fa.app_finalstatus
                ,fa.app_memoryseconds
                ,fa.app_vcoreseconds
                ,fa.app_startedtime
                ,fa.app_launchtime
                ,fa.app_finishedtime
                ,fa.app_elapsedtime
                ,fa.airflow_datechange
            FROM
                filtered_apps fa
            WHERE
                fa.rn = 1;
        """
        
        sql = f"TRUNCATE TABLE {table_name}"
        pg_hook = PostgresHook(postgres_conn_id='datamart')
        pg_hook.run(sql, True)
        logging.info('table truncated successed')

        pg_hook = PostgresHook(postgres_conn_id='datamart')
        pg_hook.run(filter_and_dedupe_sql, True)
        logging.info('success insert into db')
        return True
    

    filter_and_deduplicate_task = PythonOperator(
        task_id='filter_and_deduplicate',
        python_callable=filter_and_deduplicate,
        provide_context=True,
        dag=dag
    )

    # Task dependencies
    load_yarn_data_to_postgres_task >> filter_and_deduplicate_task