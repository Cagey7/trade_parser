import pandas as pd
from db.database import connect_to_db, init_database, get_region_by_id, check_data_stats_exists, insert_data_stats
from scraper.kgd import get_kdg_data
from scraper.statgov import get_statgov_data
from parser.excel_info import insert_data, get_insert_info, get_file_type
from db.data.countries import countries
from db.data.country_aliases import country_aliases
from db.data.init_tn_veds import init_tn_veds
from db.data.regions import regions
from db.data.country_groups import country_groups


def main():    
    conn = connect_to_db()
    cur = conn.cursor()

    init_database(cur, init_tn_veds, regions, countries, country_aliases, country_groups)

    df1 = pd.read_excel('починенные эксели кгд/31_4z_04_23.xlsx')

    insert_info = {"region_id": 11, "digit": 4, "month": 4, "year": 2023}

    region_data = get_region_by_id(cur, insert_info["region_id"])

    # try:
    if not check_data_stats_exists(cur, insert_info["region_id"], insert_info["year"], insert_info["month"], insert_info["digit"], "kgd"):

        insert_data(cur, df1, 
                    insert_info["month"], 
                    insert_info["year"], 
                    region_data["name"], 
                    get_file_type(df1))

        insert_data_stats(cur, insert_info["region_id"], insert_info["year"], insert_info["month"], insert_info["digit"], "kgd")
        conn.commit()
        print(f"Данные {region_data['name']}, {insert_info['digit']}, {insert_info['month']}, {insert_info['year']}, kgd загружены")
    else:
        print(f"Данные {region_data['name']}, {insert_info['digit']}, {insert_info['month']}, {insert_info['year']}, kgd УЖЕ В БД")
    # except:
    #     print(f"Данные {region_data['name']}, {insert_info['digit']}, {insert_info['month']}, {insert_info['year']}, kgd ОШИБКА!!!")

    # conn.commit()
    cur.close()
    conn.close()

main()
