import psycopg2

from config import host, user, password, db_name

offset = 0

connection = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    database=db_name
)

connection.autocommit = True



def create_table_users():
    """СОЗДАНИЕ ТАБЛИЦЫ USERS (НАЙДЕННЫЕ ПОЛЬЗОВАТЕЛИ"""
    with connection.cursor() as cursor:
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS users(
                id serial,
                first_name varchar(50) NOT NULL,
                last_name varchar(25) NOT NULL,
                vk_id varchar(20) NOT NULL PRIMARY KEY);"""
        )
    print("[INFO] Table USERS was created.")


def create_table_seen_users():  # references users(vk_id)
    """СОЗДАНИЕ ТАБЛИЦЫ SEEN_USERS (ПРОСМОТРЕННЫЕ ПОЛЬЗОВАТЕЛИ"""
    with connection.cursor() as cursor:
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS seen_users(
            id serial,
            vk_id varchar(50) PRIMARY KEY);"""
        )
    print("[INFO] Table SEEN_USERS was created.")


def insert_data_users(first_name, last_name, vk_id):
    """ВСТАВКА ДАННЫХ В ТАБЛИЦУ USERS"""
    with connection.cursor() as cursor:
        cursor.execute(
            f"""INSERT INTO users (first_name, last_name, vk_id)
            VALUES ('{first_name}', '{last_name}', '{vk_id}');"""
        )


def insert_data_seen_users(vk_id, offset):
    """ВСТАВКА ДАННЫХ В ТАБЛИЦУ SEEN_USERS"""
    with connection.cursor() as cursor:
        cursor.execute(
            f"""INSERT INTO seen_users (vk_id,OFFSET  )
            VALUES ('{vk_id}', '{offset}');"""
        )


def select(offset):
    """ВЫБОРКА ИЗ НЕПРОСМОТРЕННЫХ ЛЮДЕЙ"""
    with connection.cursor() as cursor:
        cursor.execute(f"""SELECT u.first_name, u.last_name, u.vk_id, su.vk_id FROM users AS u
                       LEFT JOIN seen_users AS su 
                       ON u.vk_id = su.vk_id
                       WHERE su.vk_id IS NULL
                       OFFSET '{offset}';"""
        )
        return cursor.fetchone()


def creating_database():
    create_table_users()
    create_table_seen_users()