from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
    'start_date': datetime(2021, 7, 7),
}
    
dag = DAG(
    dag_id='my_dag', 
    description='Simple DAG',
    schedule_interval=timedelta(minutes=1),
    default_args=default_args
)

local_to_hdfs = BashOperator(
    task_id='local_to_hdfs', 
    bash_command='hdfs dfs -rm /tmp/airflow/in/*; hdfs dfs -put /mnt/c/big_data/airflow/in/input.csv /tmp/airflow/in/;',
    dag=dag
)

spark_job = BashOperator(
    task_id='spark_job',
    bash_command='spark-submit --master local[*] --packages org.apache.spark:spark-avro_2.12:3.0.2 /mnt/c/big_data/airflow/simple_job.py',
    dag=dag
)

hdfs_to_local = BashOperator(
    task_id='hdfs_to_local', 
    bash_command=(
        'CURRENT_TIME=`date +%y-%m-%d_%H%M%S`; ' 
        + 'mkdir -p /mnt/c/big_data/airflow/out/${CURRENT_TIME}; ' 
        + 'hdfs dfs -get /tmp/airflow/out/* /mnt/c/big_data/airflow/out/${CURRENT_TIME}/'
    ),
    dag=dag
)

local_to_hdfs >> spark_job >> hdfs_to_local