from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'reddit',
    default_args=default_args,
    description='Start and stop Zookeeper, Kafka and spark services',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Start Zookeeper
start_zookeeper = BashOperator(
    task_id='start_zookeeper',
    bash_command='gnome-terminal -- bash -c "cd /home/sunbeam/kafka_2.13-2.7.0 && zookeeper-server-start.sh ./config/zookeeper.properties"',
    dag=dag,
)

# start Kafka
start_kafka = BashOperator(
    task_id='start_kafka',
    bash_command='sleep 5 && gnome-terminal -- bash -c "cd /home/sunbeam/kafka_2.13-2.7.0 && kafka-server-start.sh ./config/server.properties"',
    dag=dag,
)


# Run Reddit_producer.py
run_comments_producer = BashOperator(
    task_id='reddit_producer',
    bash_command='sleep 10 && gnome-terminal -- bash -c "cd /home/sunbeam/Desktop/Reddit/ && python3 reddit_producer.py"',
    dag=dag,
)

# Run stream_processor.py
run_stream_processor = BashOperator(
    task_id='stream_processor',
    bash_command='sleep 10 && gnome-terminal -- bash -c "cd /home/sunbeam/Desktop/Reddit/ && python3 stream_processor.py"',
    dag=dag,
)

# Stop all services after n minutes
stop_services = BashOperator(
    task_id='stop_services',
    bash_command='sleep 600 && gnome-terminal -- bash -c "pkill -f stream_processor.py && pkill -f reddit_producer.py && cd /home/sunbeam/kafka_2.13-2.7.0 && bin/kafka-server-stop.sh && bin/zookeeper-server-stop.sh"',
    dag=dag,
)


start_zookeeper >> start_kafka >> run_comments_producer >> run_stream_processor >> stop_services
