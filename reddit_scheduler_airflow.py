from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.time_sensor import TimeSensor
from datetime import datetime, timedelta
from pytz import timezone

# Convert IST to UTC
ist = timezone('Asia/Kolkata')
utc = timezone('UTC')

ist_time = ist.localize(datetime(2024, 8, 14, 13, 0, 0))
utc_time = ist_time.astimezone(utc)

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
    'test_v2',
    default_args=default_args,
    description='Start and stop Zookeeper, Kafka, and Cassandra services',
    schedule_interval=timedelta(days=1),
    catchup=False
)

wait_until_time = TimeSensor(
    task_id='wait_until_time',
    target_time=utc_time.time(),
    dag=dag
)

# Task to start Zookeeper in a new terminal and keep it running in the background
start_zookeeper = BashOperator(
    task_id='start_zookeeper',
    bash_command='gnome-terminal -- bash -c "cd /home/sunbeam/kafka_2.13-2.7.0 && zookeeper-server-start.sh ./config/zookeeper.properties"',
    dag=dag,
)

# Task to start Kafka in a new terminal after 5 seconds and keep it running in the background
start_kafka = BashOperator(
    task_id='start_kafka',
    bash_command='sleep 5 && gnome-terminal -- bash -c "cd /home/sunbeam/kafka_2.13-2.7.0 && kafka-server-start.sh ./config/server.properties"',
    dag=dag,
)

# Task to start Cassandra in a new terminal after 5 seconds and keep it running in the background
# start_cassandra = BashOperator(
#     task_id='start_cassandra',
#     bash_command='sleep 5 && gnome-terminal -- bash -c "cassandra &"',
#     dag=dag,
# )

# Task to run comments_producer.py in a new terminal in the foreground
run_comments_producer = BashOperator(
    task_id='run_comments_producer',
    bash_command='sleep 5 && gnome-terminal -- bash -c "cd /home/sunbeam/Desktop/project/ && python3 Reddit_producer.py"',
    dag=dag,
)

# Task to run stream_processor.py in a new terminal in the foreground
run_stream_processor = BashOperator(
    task_id='run_stream_processor',
    bash_command='gnome-terminal -- bash -c "cd /home/sunbeam/Desktop/project/ && python3 processor.py; exec bash"',
    dag=dag,
)




# Task to stop all services in order after 15 minutes
stop_services = BashOperator(
    task_id='stop_services',
    bash_command='sleep 300 && gnome-terminal -- bash -c "pkill -f stream_processor.py && pkill -f Reddit_producer.py && cd /home/sunbeam/kafka_2.13-2.7.0 && bin/kafka-server-stop.sh && bin/zookeeper-server-stop.sh"',
    dag=dag,
)

# Define task dependencies
wait_until_time >> start_zookeeper >> start_kafka >> run_comments_producer >> run_stream_processor >> stop_services
