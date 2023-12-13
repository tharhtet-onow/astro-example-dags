from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from db_modules import db_connection_work
from db_modules.chatbot_db_server import diasporaChatBotApiDomain

from airflow.models import Variable


default_args = {
    'owner': 'tharhtet',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}


@dag(
    dag_id='simple_chatbot_etl',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=days_ago(2, minute=15),
    schedule_interval=timedelta(minutes=10)
)
def chatbot_db_ETL():
    @task()
    def get_chatbot_db_query():
        
        sql_query = diasporaChatBotApiDomain().get_timestamps()
        return sql_query
    

    @task()
    def get_db_connection_string():
            
        server   = Variable.get("chat_SQL_SERVER_ONOW", default_var=None)
        database =  Variable.get("chat_SQL_DATABASE_ONOW", default_var=None)
        username =  Variable.get("chat_SQL_USERNAME_ONOW", default_var=None)
        password = Variable.get("chat_SQL_PASSWORD_ONOW", default_var=None)
        db_port  = Variable.get("chat_SQL_PORT", default_var=None)
  
        connection_str = 'SERVER={};PORT={};DATABASE={};UID={};PWD={}'.format(server,db_port,database,username,password)
        return connection_str

 

    @task()
    def extract_data_from_db(sql_query,connection_str):
        db_work = db_connection_work.db_executer(connection_str=connection_str,sql_query=sql_query)



    


    sql_query = get_chatbot_db_query()
    connection_str = get_db_connection_string()
    extract_data_from_db(sql_query = sql_query,connection_str= connection_str)

obj_chatbot_db_ETL = chatbot_db_ETL()