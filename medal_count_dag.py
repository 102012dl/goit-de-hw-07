import random
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

# Налаштування параметрів DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'medal_count_dag',
    default_args=default_args,
    description='DAG для підрахунку медалей у Olympic Dataset',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# 1. Завдання: Створення таблиці medal_counts, якщо вона не існує
create_table_sql = """
CREATE TABLE IF NOT EXISTS medal_counts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    medal_type VARCHAR(10),
    count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

create_table_task = MySqlOperator(
    task_id='create_table',
    mysql_conn_id='mysql_default',
    sql=create_table_sql,
    database='olympic_dataset',
    dag=dag,
)

# 2. Завдання: Генерація випадкового значення медалі
def choose_medal_type():
    medals = ['Bronze', 'Silver', 'Gold']
    chosen = random.choice(medals)
    print(f"Обрана медаль: {chosen}")
    return chosen

choose_medal_task = PythonOperator(
    task_id='choose_medal',
    python_callable=choose_medal_type,
    dag=dag,
)

# 3. Завдання: Розгалуження на основі вибору медалі
def branch_by_medal(**kwargs):
    chosen_medal = kwargs['ti'].xcom_pull(task_ids='choose_medal')
    if chosen_medal == 'Bronze':
        return 'count_bronze'
    elif chosen_medal == 'Silver':
        return 'count_silver'
    elif chosen_medal == 'Gold':
        return 'count_gold'
    return 'no_op'

branch_task = BranchPythonOperator(
    task_id='branch_by_medal',
    python_callable=branch_by_medal,
    provide_context=True,
    dag=dag,
)

# 4. Завдання: Підрахунок записів для кожного типу медалі та запис результату у medal_counts

# Для Bronze
count_bronze_sql = """
INSERT INTO medal_counts (medal_type, count)
SELECT 'Bronze', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Bronze';
"""
count_bronze_task = MySqlOperator(
    task_id='count_bronze',
    mysql_conn_id='mysql_default',
    sql=count_bronze_sql,
    database='olympic_dataset',
    dag=dag,
)

# Для Silver
count_silver_sql = """
INSERT INTO medal_counts (medal_type, count)
SELECT 'Silver', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Silver';
"""
count_silver_task = MySqlOperator(
    task_id='count_silver',
    mysql_conn_id='mysql_default',
    sql=count_silver_sql,
    database='olympic_dataset',
    dag=dag,
)

# Для Gold
count_gold_sql = """
INSERT INTO medal_counts (medal_type, count)
SELECT 'Gold', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Gold';
"""
count_gold_task = MySqlOperator(
    task_id='count_gold',
    mysql_conn_id='mysql_default',
    sql=count_gold_sql,
    database='olympic_dataset',
    dag=dag,
)

# 5. Завдання: Затримка виконання наступного завдання (35 секунд)
def delay_task_func():
    print("Запуск затримки 35 секунд...")
    time.sleep(35)
    print("Затримка завершена.")

delay_task = PythonOperator(
    task_id='delay_task',
    python_callable=delay_task_func,
    trigger_rule=TriggerRule.ONE_SUCCESS,  # Виконається, якщо хоча б одне з завдань підрахунку успішне
    dag=dag,
)

# 6. Завдання: Перевірка, що найновіший запис у таблиці не старший за 30 секунд
check_freshness_sql = """
SELECT COUNT(*) 
FROM medal_counts 
WHERE created_at > DATE_SUB(NOW(), INTERVAL 30 SECOND);
"""
check_freshness_task = SqlSensor(
    task_id='check_freshness',
    conn_id='mysql_default',
    sql=check_freshness_sql,
    database='olympic_dataset',
    timeout=60,
    poke_interval=5,
    mode='poke',
    dag=dag,
)

# Налаштування залежностей між завданнями:
create_table_task >> choose_medal_task >> branch_task
branch_task >> [count_bronze_task, count_silver_task, count_gold_task] >> delay_task >> check_freshness_task
