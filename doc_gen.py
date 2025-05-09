from db.database import connect_to_db
from tradeinfo.item_tables import get_data_by_country_and_year, get_table_data, sum_export_import_by_year, get_summary_data, generate_trade_turnover_text
from tradeinfo.text_gen import gen_text_flow, import_text_gen, gen_summary_text
from db.data.month_ranges import month_ranges


conn = connect_to_db()
year = 2024
country = "США"
region = "Республика Казахстан"
units_of_account = "тыс"


def generate_data_for_doc(conn, year, country, region, units_of_account):
    # Получение данных
    month, to_data = get_data_by_country_and_year(conn, region, country, year, units_of_account)

    data_for_doc = {}

    # Заголовок
    data_for_doc["document_header"] = f"Взаимная торговля {region} с {country} за {month_ranges[month]} {year} год"

    # Итоговые данные
    yearly_data = sum_export_import_by_year(to_data)
    summary_data = get_summary_data(yearly_data, month, units_of_account)
    data_for_doc["summary_table"] = summary_data
    data_for_doc["summary_text"] = gen_summary_text(summary_data[1], country, month, year, region, units_of_account)
    data_for_doc["summary_header"] = f"Показатели взаимной торговли {region} с {country}"

    # Таблицы
    data_for_doc["export_table"] = get_table_data("export", to_data, country)
    data_for_doc["import_table"] = get_table_data("import", to_data, country)
    data_for_doc["export_header"] = f"Таблица 1 – Основные товары экспорта {region} в {country}"
    data_for_doc["import_header"] = f"Таблица 2 – Основные товары импорта {region} в {country}"

    # Тексты по каждому направлению
    for flow_type in ["export", "import"]:
        data = get_table_data(flow_type, to_data, country)
        data_rev = get_table_data(flow_type, to_data, country)
        text = gen_text_flow(data, data_rev, summary_data, country, year, region, month, flow_type, units_of_account)
        data_for_doc[f"{flow_type}_text"] = text

    return month, data_for_doc


# data_for_doc = generate_data_for_doc(conn, year, country, region, units_of_account)


# print(data_for_doc["import_text"])
# print(data_for_doc["export_text"])

