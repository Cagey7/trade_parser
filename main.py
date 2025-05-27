import time
from db.database import connect_to_db, init_database, get_region_by_id, check_data_stats_exists, insert_data_stats
from scraper.kgd import get_kdg_data
from scraper.statgov import get_statgov_data
from parser.excel_info import insert_data, get_insert_info, get_file_type
from db.data.countries import countries
from db.data.country_aliases import country_aliases
from db.data.init_tn_veds import init_tn_veds
from db.data.regions import regions
from db.data.country_groups import country_groups
from db.data.categories import categories
from db.data.tn_ved_categories import tn_ved_categories


def main():    
    conn = connect_to_db()
    cur = conn.cursor()

    init_database(cur, init_tn_veds, regions, countries, country_aliases, country_groups, categories, tn_ved_categories)
    for insert_info in get_insert_info():
        region_data = get_region_by_id(cur, insert_info["region_id"])

        try:
            if not check_data_stats_exists(cur, insert_info["region_id"], insert_info["year"], insert_info["month"], insert_info["digit"], "kgd"):
                df1 = get_kdg_data(region_data["kgd_code"], 
                                f"{insert_info['digit']}z", 
                                str(insert_info["month"]).zfill(2), 
                                str(insert_info["year"]))

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
        except:
            print(f"Данные {region_data['name']}, {insert_info['digit']}, {insert_info['month']}, {insert_info['year']}, kgd ОШИБКА!!!")

            continue
        

        try:
            if not check_data_stats_exists(cur, insert_info["region_id"], insert_info["year"], insert_info["month"], insert_info["digit"], "statgov"):
                df = get_statgov_data(region_data["statgov_code"],
                                    region_data["kgd_code"], 
                                    insert_info["digit"], 
                                    str(insert_info["month"]).zfill(2), 
                                    str(insert_info["year"]))
                insert_data(cur, df, 
                            insert_info["month"], 
                            insert_info["year"], 
                            region_data["name"], 
                            get_file_type(df))

                insert_data_stats(cur, insert_info['region_id'], insert_info['year'], insert_info['month'], insert_info['digit'], "statgov")
                conn.commit()
                print(f"Данные {region_data['name']}, {insert_info['digit']}, {insert_info['month']}, {insert_info['year']}, statgov загружены")
            else:
                print(f"Данные {region_data['name']}, {insert_info['digit']}, {insert_info['month']}, {insert_info['year']}, statgov УЖЕ В БД")
        except:
            print(f"Данные {region_data['name']}, {insert_info['digit']}, {insert_info['month']}, {insert_info['year']}, statgov ОШИБКА!!!")
            continue
        
    cur.close()
    conn.close()

main()
