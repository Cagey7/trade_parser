from collections import defaultdict

def get_data_by_country_and_year(conn, region, name_ru, year):

    try:
        with conn.cursor() as cursor:
            # 1. Получаем доступные месяцы за указанный год
            month_query = """
                SELECT DISTINCT month
                FROM data d
                JOIN countries c ON d.country_id = c.id
                JOIN regions r ON d.region_id = r.id
                WHERE c.name_ru = %s
                  AND r.name = %s
                  AND d.year = %s
                ORDER BY month;
            """

            cursor.execute(month_query, (name_ru, region, year))
            months = [row[0] for row in cursor.fetchall()]


            if not months:
                return []  # Нет данных за указанный год

            # 2. Основной запрос с фильтром по месяцам
            data_query = f"""
                SELECT 
                    SUM(d.export_tonn) AS total_ex_tonn,
                    SUM(d.export_units) AS total_ex_ad_un,
                    SUM(d.export_value) AS total_ex_value,
                    SUM(d.import_tonn) AS total_im_tonn,
                    SUM(d.import_units) AS total_im_ad_un,
                    SUM(d.import_value) AS total_im_value,
                    c.name_ru AS country_name,
                    r.name AS region_name,
                    tv.code AS tn_ved_code,
                    tv.name AS tn_ved_name,
                    tv.measure AS tn_ved_measure,
                    d.year
                FROM data d
                JOIN countries c ON d.country_id = c.id
                JOIN regions r ON d.region_id = r.id
                JOIN tn_veds tv ON d.tn_ved_id = tv.id
                WHERE c.name_ru = %s
                AND r.name = %s
                AND d.year IN (%s, %s)
                AND d.month IN ({','.join(['%s'] * len(months))})
                GROUP BY c.name_ru, r.name, tv.code, tv.name, tv.measure, d.year;
            """

            # Формируем параметры: страна, год, год-1, и месяцы
            params = [name_ru, region, year, year - 1] + months
            cursor.execute(data_query, params)
            rows = cursor.fetchall()

            # Список имён колонок (в том же порядке, что и SELECT)
            columns = [
                "export_tons", "export_units", "export_value",
                "import_tons", "import_units", "import_value",
                "country", "region",
                "tn_ved_code", "tn_ved_name", "tn_ved_measure",
                "year"
            ]

            # Преобразуем в список словарей
            results = [dict(zip(columns, row)) for row in rows]

        return months[-1], results
    finally:
        conn.close()


def sum_by_key(data, key):
    return sum(x[key] for x in data if isinstance(x.get(key), (int, float)))


def sort_by_key(data, key, reverse=False):
    filtered_data = [x for x in data if x.get(key) is not None]
    return sorted(filtered_data, key=lambda x: x[key], reverse=reverse)


def find_by_tn_ved_code(data, code):
    for item in data:
        if item.get('tn_ved_code') == code:
            return item
    return None


def split_by_year(data):
    # Извлекаем уникальные года и сортируем их
    years = sorted({entry['year'] for entry in data})
    if len(years) != 2:
        raise ValueError("В списке должно быть ровно два разных года.")

    year1, year2 = years
    list1 = [entry for entry in data if entry['year'] == year1]
    list2 = [entry for entry in data if entry['year'] == year2]

    return list1, list2


def fn(num):
    if isinstance(num, float) and num.is_integer():
        return int(num)
    return num


def gen_row_data(index,
                 flow_type,
                 larg_year_sum, 
                 prev_year_sum, 
                 larg_year_dict, 
                 prev_year_list):
    
    larg_year_tn_ved_name = larg_year_dict["tn_ved_name"]
    larg_year_tn_ved_code = larg_year_dict["tn_ved_code"]
    larg_year_value = larg_year_dict[f"{flow_type}_value"]
    measure = larg_year_dict.get('tn_ved_measure')
    measure_str = measure.lower() if measure else 'тонна'
    if larg_year_dict[f"{flow_type}_units"]:
        larg_year_tons = larg_year_dict[f"{flow_type}_units"]
    else:
        larg_year_tons = larg_year_dict[f"{flow_type}_tons"]


    info_cell = f"{index}. {larg_year_tn_ved_name} (код {larg_year_tn_ved_code} ТНВЭД), {measure_str}"
    prev_year_dict = find_by_tn_ved_code(prev_year_list, larg_year_tn_ved_code)


    if prev_year_dict and prev_year_dict[f"{flow_type}_value"]:
        prev_year_value = prev_year_dict[f"{flow_type}_value"]
        if larg_year_dict[f"{flow_type}_units"]:
            prev_year_tons = prev_year_dict[f"{flow_type}_units"]
        else:
            prev_year_tons = prev_year_dict[f"{flow_type}_tons"]

        prev_value = fn(round(prev_year_value, 1))
        prev_tons = fn(round(prev_year_tons, 1))

        share_tons = prev_year_value / prev_year_sum * 100
        share_tons_value = f"{fn(round(share_tons, 2 if share_tons < 1 else 1))}%"
        
        if larg_year_value > prev_year_value:
            growth_value = fn(round(larg_year_value / prev_year_value, 3))
            if growth_value > 2:
                growth_value = f"рост в {fn(round(growth_value, 1))} р."
            else:
                growth_value = f"+{fn(round((growth_value - 1)*100, 1))}%"
        else:
            growth_value = f"{fn(round(((larg_year_value / prev_year_value) - 1) * 100, 1))}%"

        
        if larg_year_tons > prev_year_tons:
            growth_tons = fn(round(larg_year_tons / prev_year_tons, 3))
            if growth_tons > 2:
                growth_tons = f"рост в {fn(round(growth_tons, 1))} р."
            else:
                growth_tons = f"+{fn(round((growth_tons - 1)*100, 1))}%"
        else:
            growth_tons = f"{fn(round(((larg_year_tons / prev_year_tons) - 1) * 100, 1))}%"
    
    else:
        prev_value = 0
        prev_tons = 0
        share_tons_value = "-"
        growth_value = "new"
        growth_tons = "new"

    share_larg = larg_year_value / larg_year_sum * 100
    share_larg_value = f"{fn(round(share_larg, 2 if share_larg < 1 else 1))}%"


    return [
        info_cell,
        str(prev_tons),
        str(prev_value),
        share_tons_value,
        str(round(larg_year_tons, 1)),
        str(round(larg_year_value, 1)),
        share_larg_value,
        growth_tons,
        growth_value
    ]



def get_table_data(flow_type, to_data, country):
    data_for_doc = []

    prev_year_list, larg_year_list = split_by_year(to_data)
    sorted_by_im_value = sort_by_key(larg_year_list, f"{flow_type}_value", True)


    larg_year_sum = sum_by_key(sorted_by_im_value, f"{flow_type}_value")
    prev_year_sum = sum_by_key(prev_year_list, f"{flow_type}_value")

    if flow_type == "export":
        str_type = "экспорта"
    elif flow_type == "import":
        str_type = "импорта"

    if larg_year_sum > prev_year_sum:
        growth_country = fn(round(larg_year_sum / prev_year_sum, 3))
        if growth_country > 2:
            growth_country = f"рост в {fn(round(growth_country, 1))} р."
        else:
            growth_country = f"+{fn(round((growth_country - 1)*100, 1))}%"
    else:
        growth_country = f"{fn(round(((larg_year_sum / prev_year_sum) - 1) * 100, 1))}%"

    first_data_row = [f"Всего {str_type} в {country}", "", str(round(prev_year_sum, 1)), "100%", "", str(round(larg_year_sum)), "100%", "", growth_country]
    data_for_doc.append(first_data_row)

    for i, row in enumerate(sorted_by_im_value):
        new_row = gen_row_data(i+1, flow_type,
                            larg_year_sum,
                            prev_year_sum,
                            row,
                            prev_year_list)
        data_for_doc.append(new_row)

    return data_for_doc


def sum_export_import_by_year(results):
    year_sums = defaultdict(lambda: {"export_value": 0, "import_value": 0})

    for row in results:
        year = row["year"]
        year_sums[year]["export_value"] += row.get("export_value") or 0
        year_sums[year]["import_value"] += row.get("import_value") or 0

    return dict(year_sums)


def get_main_table_data(yearly_data, month):

    years = sorted(yearly_data.keys())
    prev_year, curr_year = years

    prev_export = yearly_data[prev_year]['export_value']
    curr_export = yearly_data[curr_year]['export_value']
    prev_import = yearly_data[prev_year]['import_value']
    curr_import = yearly_data[curr_year]['import_value']

    prev_trade = prev_export + prev_import
    curr_trade = curr_export + curr_import

    prev_balance = prev_export - prev_import
    curr_balance = curr_export - curr_import

    def calc_growth(new, old, percent=True):
        if old == 0:
            return "new"
        diff = new - old
        ratio = diff / old * 100
        rounded = round(ratio, 1)
        if percent:
            return f"+{rounded}%" if rounded > 0 else f"{rounded}%"
        return rounded

    def format_balance_change(curr, prev):
        if curr > prev:
            return "улучшился"
        elif curr < prev:
            return "ухудшился"
        else:
            return "без изменений"

    table = [
        ["млн. долл. США", f"{prev_year} год", f"{curr_year} год", f"Прирост {curr_year}/{prev_year}"],
        ["Товарооборот", f"{round(prev_trade / 1000, 1)}", f"{round(curr_trade / 1000, 1)}", calc_growth(curr_trade, prev_trade)],
        ["Экспорт", f"{round(prev_export / 1000, 1)}", f"{round(curr_export / 1000, 1)}", calc_growth(curr_export, prev_export)],
        ["Импорт", f"{round(prev_import / 1000, 1)}", f"{round(curr_import / 1000, 1)}", calc_growth(curr_import, prev_import)],
        ["Торговый баланс", f"{round(prev_balance / 1000, 1)}", f"{round(curr_balance / 1000, 1)}", format_balance_change(curr_balance, prev_balance)]
    ]

    return table


def generate_trade_turnover_text(year_sums, country, region):

    years = sorted(year_sums.keys())
    prev_year, curr_year = years

    prev_trade = year_sums[prev_year]['export_value'] + year_sums[prev_year]['import_value']
    curr_trade = year_sums[curr_year]['export_value'] + year_sums[curr_year]['import_value']

    if prev_trade == 0:
        change_text = "данные за предыдущий год отсутствуют"
    else:
        change_ratio = (curr_trade - prev_trade) / prev_trade * 100
        direction = "выше" if change_ratio > 0 else "ниже"
        change_percent = f"{abs(round(change_ratio, 1))}%"
        change_text = f"что на {change_percent} {direction}, по сравнению с предыдущим годом ({round(prev_trade / 1000, 1)} млн. долл. США)"

    result = (
        f"Товарооборот между {region} и {country} за {curr_year} год составил "
        f"{round(curr_trade / 1000, 1)} млн. долл. США, {change_text}."
    )
    return result
