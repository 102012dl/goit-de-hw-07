import random
import time
from datetime import datetime, timedelta
from typing import Optional, Literal

import mysql.connector
from prefect import flow, task
from prefect.tasks import task_input_hash
from sqlalchemy import create_engine, text


# Налаштування підключення до MySQL
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "password",
    "database": "olympic_dataset",
    "port": 3306
}


@task(name="Створення таблиці")
def create_table() -> None:
    """Створення таблиці для зберігання результатів підрахунку медалей"""
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS medals_count (
        id INT AUTO_INCREMENT PRIMARY KEY,
        medal_type VARCHAR(10),
        count INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    cursor.execute(create_table_query)
    conn.commit()
    
    print("Таблицю medals_count успішно створено (або вона вже існує)")
    
    cursor.close()
    conn.close()


@task(name="Випадковий вибір типу медалі")
def choose_medal_type() -> str:
    """Випадково обирає один із трьох типів медалей"""
    medals = ['Bronze', 'Silver', 'Gold']
    chosen_medal = random.choice(medals)
    print(f"Обрано тип медалі: {chosen_medal}")
    return chosen_medal


@task(name="Підрахунок бронзових медалей")
def count_bronze_medals() -> None:
    """Підрахунок бронзових медалей у таблиці athlete_event_results"""
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    query = """
    INSERT INTO medals_count (medal_type, count)
    SELECT 'Bronze', COUNT(*) 
    FROM olympic_dataset.athlete_event_results 
    WHERE medal = 'Bronze';
    """
    
    cursor.execute(query)
    conn.commit()
    
    print("Підрахунок бронзових медалей завершено")
    
    cursor.close()
    conn.close()


@task(name="Підрахунок срібних медалей")
def count_silver_medals() -> None:
    """Підрахунок срібних медалей у таблиці athlete_event_results"""
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    query = """
    INSERT INTO medals_count (medal_type, count)
    SELECT 'Silver', COUNT(*) 
    FROM olympic_dataset.athlete_event_results 
    WHERE medal = 'Silver';
    """
    
    cursor.execute(query)
    conn.commit()
    
    print("Підрахунок срібних медалей завершено")
    
    cursor.close()
    conn.close()


@task(name="Підрахунок золотих медалей")
def count_gold_medals() -> None:
    """Підрахунок золотих медалей у таблиці athlete_event_results"""
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    query = """
    INSERT INTO medals_count (medal_type, count)
    SELECT 'Gold', COUNT(*) 
    FROM olympic_dataset.athlete_event_results 
    WHERE medal = 'Gold';
    """
    
    cursor.execute(query)
    conn.commit()
    
    print("Підрахунок золотих медалей завершено")
    
    cursor.close()
    conn.close()


@task(name="Затримка виконання")
def delay_task(seconds: int = 35) -> None:
    """Затримка виконання на вказану кількість секунд"""
    print(f"Очікування {seconds} секунд...")
    time.sleep(seconds)
    print("Затримку завершено!")


@task(name="Перевірка свіжості запису")
def check_recent_record(timeout: int = 60, interval: int = 5) -> bool:
    """
    Перевіряє чи є запис в таблиці medals_count не старший за 30 секунд.
    Перевірка виконується з інтервалом `interval` секунд, загальний час очікування - `timeout` секунд.
    """
    engine = create_engine(f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with engine.connect() as connection:
                query = text("""
                    SELECT 1 FROM medals_count
                    WHERE created_at > DATE_SUB(NOW(), INTERVAL 30 SECOND)
                    LIMIT 1
                """)
                result = connection.execute(query)
                if result.fetchone():
                    print("Знайдено свіжий запис (не старший за 30 секунд)")
                    return True
                
            print(f"Свіжий запис не знайдено. Очікування {interval} секунд...")
            time.sleep(interval)
        except Exception as e:
            print(f"Помилка при перевірці запису: {e}")
            return False
    
    print("Час очікування минув. Свіжий запис не знайдено.")
    return False


@flow(name="Olympic Medals Flow")
def olympic_medals_flow(delay_seconds: int = 35) -> None:
    """
    Головний потік для підрахунку олімпійських медалей.
    
    Параметри:
    ----------
    delay_seconds : int, optional
        Кількість секунд для затримки перед перевіркою свіжості запису.
        За замовчуванням 35 секунд.
    """
    # Крок 1: Створення таблиці
    create_table()
    
    # Крок 2: Випадковий вибір типу медалі
    medal_type = choose_medal_type()
    
    # Крок 3 і 4: Розгалуження та підрахунок медалей
    if medal_type == 'Bronze':
        count_bronze_medals()
    elif medal_type == 'Silver':
        count_silver_medals()
    else:  # Gold
        count_gold_medals()
    
    # Крок 5: Затримка виконання
    delay_task(delay_seconds)
    
    # Крок 6: Перевірка свіжості запису
    success = check_recent_record()
    
    if success:
        print("Потік успішно завершено!")
    else:
        print("Потік завершено з помилкою: свіжий запис не знайдено.")


if __name__ == "__main__":
    # Запуск потоку з різними параметрами затримки
    print("\n=== Запуск потоку з затримкою 10 секунд (повинен успішно завершитись) ===\n")
    olympic_medals_flow(delay_seconds=10)
    
    print("\n=== Запуск потоку з затримкою 35 секунд (повинен завершитись з помилкою) ===\n")
    olympic_medals_flow(delay_seconds=35)
