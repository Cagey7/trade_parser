import psycopg2
from datetime import datetime
from db.queries.init import create_tables_sql, check_database_sql
from db.queries.countries import get_country_names_all_sql, insert_country_sql, get_country_id_by_iso_sql, insert_country_alias_sql
from db.queries.tnveds import get_tn_ved_dict_sql, insert_tn_ved_sql, update_measure_sql
from db.queries.regions import get_region_dict_sql, insert_region_sql, get_region_by_id_sql
from db.queries.stats import check_data_stats_exists_sql, insert_or_update_data_stats_sql
from db.queries.data import insert_data_sql


DB_NAME = "trade100"
DB_USER = "postgres"
DB_PASSWORD = "123456"
DB_HOST = "localhost"
DB_PORT = "5433"


# Подключение и открытие экселя
def connect_to_db():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    return conn


# Инициализация базы данных
def init_database(cur, init_tn_veds, regions, countries, country_aliases):
    cur.execute(check_database_sql)
    exists = cur.fetchone()[0]
    if exists:
        print("База данных уже инициализирована — таблица tn_veds найдена.")
        return

    cur.execute(create_tables_sql)
    insert_tn_veds(cur, init_tn_veds)
    insert_regions(cur, regions)
    insert_counries(cur, countries, country_aliases)

# Получение данных по id
def get_country_dic(cur):
    cur.execute(get_country_names_all_sql)
    rows = cur.fetchall()
    return {name.upper(): country_id for name, country_id in rows}


def get_tn_ved_dic(cur):
    cur.execute(get_tn_ved_dict_sql)
    rows = cur.fetchall()
    return {row[0]: row[1] for row in rows}


def get_region_dic(cur):
    cur.execute(get_region_dict_sql)
    rows = cur.fetchall()
    return {row[0]: row[1] for row in rows}


def insert_tn_veds(cur, init_tn_veds):
    for code, name in init_tn_veds.items():
        cur.execute(insert_tn_ved_sql, (name, code.strip(), len(code.strip())))


def insert_regions(cur, regions):
    for region in regions:
        name = region["name"]
        kgd_code = region["kgd_code"]
        statgov_code = region["statgov_code"]
        statgov_name = region["statgov_name"]
        cur.execute(insert_region_sql, (name, kgd_code, statgov_code, statgov_name))


def insert_counries(cur, countries, country_aliases):
    for country in countries:
        cur.execute(insert_country_sql, (country['code'], country['name_ru'], country['name_eng']))
    
    for alias in country_aliases:
        cur.execute(get_country_id_by_iso_sql, (alias['code'],))
        result = cur.fetchone()
        if result:
            country_id = result[0]
            cur.execute(insert_country_alias_sql, (country_id, alias['name']))
        else:
            print(f"Страна с кодом {alias['code']} не найдена!")



def check_data_stats_exists(cur, region_id, year, month, digit, source_type):
    cur.execute(check_data_stats_exists_sql, (region_id, year, month, digit, source_type))
    return cur.fetchone() is not None


def get_region_by_id(cur, region_id):
    cur.execute(get_region_by_id_sql, (region_id,))
    result = cur.fetchone()
    if result:
        return {
            "id": result[0],
            "name": result[1],
            "kgd_code": result[2],
            "statgov_code": result[3],
            "statgov_name": result[4]
        }
    else:
        return None


def insert_data_stats(cur, region_id, year, month, digit, source_type):
    cur.execute(insert_or_update_data_stats_sql, (region_id, year, month, digit, source_type, datetime.now()))


def update_measure(cur, code, new_measure):
    cur.execute(update_measure_sql, (new_measure, code))


def insert_trade_data(cur, values):
    cur.execute(insert_data_sql, values)