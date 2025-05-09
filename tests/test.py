import os
import pytest
import psycopg2
from decimal import Decimal
import math


EXPECTED_IM_VALUES = {
    'Республика Казахстан': {
        (2023, 1): 3_031_184,
        (2023, 2): 3_216_175,
        (2023, 3): 3_786_231,
        (2023, 4): 3_541_790,
        (2023, 5): 3_754_049,
        (2023, 6): 3_615_781,
        (2023, 7): 3_842_962,
        (2023, 8): 3_687_533,
        (2023, 9): 3_293_023,
        (2023, 10): 3_515_798,
        (2023, 11): 3_507_690,
        (2023, 12): 3_398_563,
        (2024, 1): 3_063_543,
        (2024, 2): 2_732_402,
        (2024, 3): 2_948_488,
        (2024, 4): 3_682_158,
        (2024, 5): 3_611_406,
        (2024, 6): 3_133_979,
        (2024, 7): 3_523_729,
        (2024, 8): 3_410_079,
        (2024, 9): 3_231_135,
        (2024, 10): 3_682_380,
        (2024, 11): 3_499_215,
        (2024, 12): 3_798_995,
        (2025, 1): 2_872_699,
        (2025, 2): 2_877_422,
    }
}

EXPECTED_IM_VALUES_CU = {
    'Республика Казахстан': {
        (2023, 1): 1_229_970,
        (2023, 2): 1_375_678,
        (2023, 3): 1_629_068,
        (2023, 4): 1_510_795,
        (2023, 5): 1_666_364,
        (2023, 6): 1_533_444,
        (2023, 7): 1_537_001,
        (2023, 8): 1_421_339,
        (2023, 9): 1_432_235,
        (2023, 10): 1_458_677,
        (2023, 11): 1_612_440,
        (2023, 12): 1_814_484,
        (2024, 1): 1_222_574,
        (2024, 2): 1_336_409,
        (2024, 3): 1_603_530,
        (2024, 4): 1_490_743,
        (2024, 5): 1_564_303,
        (2024, 6): 1_773_357,
        (2024, 7): 1_694_160,
        (2024, 8): 1_773_714,
        (2024, 9): 1_677_154,
        (2024, 10): 1_759_762,
        (2024, 11): 1_687_132,
        (2024, 12): 1_886_950,
        (2025, 1): 1_162_499,
        (2025, 2): 1_348_819,
    }
}


@pytest.fixture(scope="session")
def db_config():
    """
    Возвращает конфигурацию подключения к БД из переменных окружения.
    Переменные по умолчанию можно переопределить при запуске.
    """
    return {
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": os.getenv("DB_PORT", "5432"),
        "dbname": os.getenv("DB_NAME", "trade100"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "123456"),
    }

@pytest.fixture(scope="session")
def db_connection(db_config):
    """
    Устанавливает соединение с базой данных на уровне сессии pytest.
    После завершения тестов соединение закрывается.
    """
    conn = psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
    )
    yield conn
    conn.close()

@pytest.fixture(scope="function")
def cursor(db_connection):
    """
    Создает курсор для каждого тестового случая.
    После теста выполняет rollback, чтобы не сохранять изменения.
    """
    cur = db_connection.cursor()
    yield cur
    db_connection.rollback()
    cur.close()

def test_connection(cursor):
    """
    Проверка базового запроса: SELECT 1
    """
    cursor.execute("SELECT 1;")
    result = cursor.fetchone()
    assert result[0] == 1


import pytest
import math

@pytest.mark.parametrize("region, year, month, expected_int", [
    (region, year, month, expected)
    for region, values in EXPECTED_IM_VALUES.items()
    for (year, month), expected in values.items()
])
def test_sum_im_value_for_region_each_month_integer_part(cursor, region, year, month, expected_int):
    query = """
        SELECT SUM(d.import_value)
        FROM data d
        JOIN regions r ON d.region_id = r.id
        JOIN countries c ON d.country_id = c.id
        WHERE r.name = %s
          AND d.year = %s
          AND d.month = %s
          AND c.name_ru NOT IN ('Россия', 'Кыргызстан', 'Армения', 'Беларусь');
    """
    cursor.execute(query, (region, year, month))
    result = cursor.fetchone()

    assert result is not None and result[0] is not None, f"{year}-{month}: No data returned"

    actual_decimal = result[0]
    actual_int = math.trunc(actual_decimal)

    assert abs(actual_int - expected_int) <= 6, (
        f"{year}-{month}: expected ~{expected_int} (±6), got {actual_int} from {actual_decimal}"
    )


@pytest.mark.parametrize("region, year, month, expected_int", [
    (region, year, month, expected)
    for region, values in EXPECTED_IM_VALUES_CU.items()
    for (year, month), expected in values.items()
])
def test_sum_im_value_for_cu_countries_only(cursor, region, year, month, expected_int):
    query = """
        SELECT SUM(d.import_value)
        FROM data d
        JOIN regions r ON d.region_id = r.id
        JOIN countries c ON d.country_id = c.id
        WHERE r.name = %s
          AND d.year = %s
          AND d.month = %s
          AND c.name_ru IN ('Россия', 'Кыргызстан', 'Армения', 'Беларусь');
    """
    cursor.execute(query, (region, year, month))
    result = cursor.fetchone()

    assert result is not None and result[0] is not None, f"{year}-{month}: No data returned"

    actual_decimal = result[0]
    actual_int = math.trunc(actual_decimal)

    assert abs(actual_int - expected_int) <= 0, (
        f"{year}-{month}: expected exactly {expected_int}, got {actual_int} from {actual_decimal}"
    )
