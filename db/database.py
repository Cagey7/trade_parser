import psycopg2
from datetime import datetime
from db.queries.init import create_tables_sql, check_database_sql
from db.queries.countries import get_country_names_all_sql, insert_country_sql, get_country_id_by_iso_sql, insert_country_alias_sql, get_get_country_id_by_name_or_alias_sql
from db.queries.tnveds import get_tn_ved_dict_sql, insert_tn_ved_sql, update_measure_sql
from db.queries.regions import get_region_dict_sql, insert_region_sql, get_region_by_id_sql
from db.queries.stats import check_data_stats_exists_sql, insert_or_update_data_stats_sql
from db.queries.data import insert_data_sql
from db.queries.categories import *


DB_NAME = "trade_test1"
DB_USER = "postgres"
DB_PASSWORD = "123456"
DB_HOST = "localhost"
DB_PORT = "5433"

# DB_NAME = "trade_new_2025"
# DB_USER = "postgres"
# DB_PASSWORD = "forestlampsilver"
# DB_HOST = "13.60.76.175"
# DB_PORT = "5432"

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
def init_database(cur, init_tn_veds, regions, countries, country_aliases, categories, tn_ved_categories, groups_country_groups, group_country_groups_data):
    cur.execute(check_database_sql)
    exists = cur.fetchone()[0]
    if exists:
        print("База данных уже инициализирована — таблица tn_veds найдена.")
        return

    cur.execute(create_tables_sql)
    insert_tn_veds(cur, init_tn_veds)
    insert_regions(cur, regions)
    insert_counries(cur, countries, country_aliases)
    insert_categories(cur, categories)
    insert_tn_ved_categories(cur, tn_ved_categories)
    insert_country_groups(cur, groups_country_groups)
    insert_groups_country_groups(cur, group_country_groups_data)

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


def get_country_id_by_name_or_alias(cur, name):
    cur.execute(get_get_country_id_by_name_or_alias_sql, (name, name))
    
    result = cur.fetchone()
    return result[0] if result else None


def insert_tn_ved_category(cur, name, parent_id=None):
    cur.execute(insert_tn_ved_categories_sql, (name, parent_id))
    result = cur.fetchone()
    return result[0] if result else None


def insert_tn_ved_to_category(cur, tn_ved_id, category_id):
    cur.execute(insert_tn_ved_category_map_sql, (tn_ved_id, category_id))


def insert_categories(cur, categories):
    for name, parent_name in categories:
        print(name, parent_name)
        if parent_name:
            cur.execute("SELECT id FROM tn_ved_categories WHERE name = %s;", (parent_name,))
            result = cur.fetchone()
            parent_id = result[0] if result else None
        else:
            parent_id = None

        cur.execute("""
            INSERT INTO tn_ved_categories (name, parent_id)
            VALUES (%s, %s)
            ON CONFLICT (name) DO NOTHING;
        """, (name, parent_id))
        


def insert_tn_ved_categories(cur, tn_ved_categories):
    for code, category_name in tn_ved_categories:
        cur.execute("SELECT id FROM tn_veds WHERE code = %s;", (code,))
        tn_ved_row = cur.fetchone()
        if not tn_ved_row:
            print(f"Код ТН ВЭД {code} не найден в базе.")
            continue
        tn_ved_id = tn_ved_row[0]

        cur.execute("SELECT id FROM tn_ved_categories WHERE name = %s;", (category_name,))
        category_row = cur.fetchone()
        if not category_row:
            print(f"Категория '{category_name}' не найдена в базе.")
            continue
        category_id = category_row[0]


        cur.execute("""
            INSERT INTO tn_ved_category_map (tn_ved_id, tn_ved_category_id)
            VALUES (%s, %s)
            ON CONFLICT (tn_ved_id, tn_ved_category_id) DO NOTHING;
        """, (tn_ved_id, category_id))


def insert_country_groups(cur, groups_country_groups):
    for name, country_group in groups_country_groups:
        print(name, country_group)
        if country_group:
            cur.execute("SELECT id FROM country_groups WHERE name = %s;", (country_group,))
            result = cur.fetchone()
            parent_id = result[0] if result else None
        else:
            parent_id = None

        cur.execute("""
            INSERT INTO country_groups (name, parent_id)
            VALUES (%s, %s)
            ON CONFLICT (name) DO NOTHING;
        """, (name, parent_id))


def insert_groups_country_groups(cur, group_country_groups_data):
    for country, group_country_groups in group_country_groups_data:
        cur.execute("SELECT id FROM countries WHERE name_ru = %s;", (country,))
        country_row = cur.fetchone()
        if not country_row:
            print(f"Код ТН ВЭД {country} не найден в базе.")
            continue
        country_id = country_row[0]

        cur.execute("SELECT id FROM country_groups WHERE name = %s;", (group_country_groups,))
        group_row = cur.fetchone()
        if not group_row:
            print(f"Страна '{group_country_groups}' не найдена в базе.")
            continue
        group_id = group_row[0]

        cur.execute("""
            INSERT INTO country_group_membership (country_id, country_group_id)
            VALUES (%s, %s)
            ON CONFLICT (country_id, country_group_id) DO NOTHING;
        """, (country_id, group_id))
