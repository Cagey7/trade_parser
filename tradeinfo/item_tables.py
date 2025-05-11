from collections import defaultdict
from tradeinfo.text_gen import num_converter, fn
from db.data.month_ranges import month_ranges


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
                return 0, []  # Нет данных за указанный год

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
            params = [name_ru, region, year, year - 1] + months
            cursor.execute(data_query, params)
            rows = cursor.fetchall()

            columns = [
                "export_tons", "export_units", "export_value",
                "import_tons", "import_units", "import_value",
                "country", "region",
                "tn_ved_code", "tn_ved_name", "tn_ved_measure",
                "year"
            ]

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


def cn(num):
    if isinstance(num, float) and num.is_integer():
        return int(num)
    return num



def calc_growth(current, previous):
    if not previous:
        return "new"
    ratio = current / previous
    if ratio > 2:
        return f"рост в {cn(round(ratio, 1))} р."
    delta = (ratio - 1) * 100
    if delta > 0:
        return f"+{cn(round(delta, 1))}%"
    return f"{cn(round(delta, 1))}%"

def calc_share(value, total):
    if not total:
        return "-"
    share = value / total * 100
    return f"{cn(round(share, 2 if share < 1 else 1))}%"

def get_quantity(entry, gen_type):
    return entry.get(f"{gen_type}_units") or entry.get(f"{gen_type}_tons") or 0

def find_by_tn_ved_code(data, code):
    for item in data:
        if item.get('tn_ved_code') == code:
            return item
    return None


def gen_row_data(index,
                 gen_type,
                 larg_year_sum, 
                 prev_year_sum, 
                 larg_year_dict, 
                 prev_year_list):

    # Данные по текущему году
    larg_year_tn_ved_name = larg_year_dict["tn_ved_name"]
    larg_year_tn_ved_code = larg_year_dict["tn_ved_code"]
    larg_year_value = larg_year_dict.get(f"{gen_type}_value", 0)
    larg_year_tons = get_quantity(larg_year_dict, gen_type)
    measure = larg_year_dict.get('tn_ved_measure')
    measure_str = measure.lower() if measure else 'тонна'

    # Описание ТНВЭД
    info_cell = f"{index}. {larg_year_tn_ved_name} (код {larg_year_tn_ved_code} ТНВЭД), {measure_str}"

    # Данные за предыдущий год
    prev_year_dict = find_by_tn_ved_code(prev_year_list, larg_year_tn_ved_code)

    if prev_year_dict and prev_year_dict.get(f"{gen_type}_value"):
        prev_year_value = prev_year_dict[f"{gen_type}_value"]
        prev_year_tons = get_quantity(prev_year_dict, gen_type)

        prev_value = cn(round(prev_year_value, 1))
        prev_tons = cn(round(prev_year_tons, 1))

        share_tons_value = calc_share(prev_year_value, prev_year_sum)
        growth_value = calc_growth(larg_year_value, prev_year_value)
        growth_tons = calc_growth(larg_year_tons, prev_year_tons)
    else:
        prev_value = 0
        prev_tons = 0
        share_tons_value = "-"
        growth_value = "new"
        growth_tons = "new"

    share_larg_value = calc_share(larg_year_value, larg_year_sum)

    return [
        info_cell,
        fn(prev_tons),
        str(prev_value),
        share_tons_value,
        fn(round(larg_year_tons, 1)),
        str(round(larg_year_value, 1)),
        share_larg_value,
        growth_tons,
        growth_value,
        prev_value - larg_year_value
    ]



def get_table_data(gen_type, to_data, country, reverse=False):
    data_for_doc = []

    if reverse:
        larg_year_list, prev_year_list = split_by_year(to_data)
    else:
        prev_year_list, larg_year_list = split_by_year(to_data)
        
        
    sorted_by_im_value = sort_by_key(larg_year_list, f"{gen_type}_value", True)


    larg_year_sum = sum_by_key(sorted_by_im_value, f"{gen_type}_value")
    prev_year_sum = sum_by_key(prev_year_list, f"{gen_type}_value")
    if gen_type == "export":
        str_type = "экспорта"
    elif gen_type == "import":
        str_type = "импорта"


    if prev_year_sum == 0:
        growth_country = "+100%"
    elif larg_year_sum > prev_year_sum:
        growth_country = cn(round(larg_year_sum / prev_year_sum, 3))
        if growth_country > 2:
            growth_country = f"рост в {cn(round(growth_country, 1))} р."
        else:
            growth_country = f"+{cn(round((growth_country - 1)*100, 1))}%"
    else:
        growth_country = f"{cn(round(((larg_year_sum / prev_year_sum) - 1) * 100, 1))}%"

    first_data_row = [
        f"Всего {str_type} в {country}",
        "",
        str(round(prev_year_sum, 1)),
        "-" if prev_year_sum == 0 else "100%",
        "",
        str(round(larg_year_sum)),
        "-" if larg_year_sum == 0 else "100%",
        "",
        growth_country
    ]
    data_for_doc.append(first_data_row)

    for i, row in enumerate(sorted_by_im_value):
        new_row = gen_row_data(i+1, gen_type,
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


def get_summary_data(yearly_data, month):

    years = sorted(yearly_data.keys())
    prev_year, curr_year = years

    prev_export = yearly_data[prev_year]['export_value']
    curr_export = yearly_data[curr_year]['export_value']
    prev_import = yearly_data[prev_year]['import_value']
    curr_import = yearly_data[curr_year]['import_value']

    values = [prev_export, curr_export, prev_import, curr_import]
    max_value = max(values)
    min_value = min(values)
    if min_value == 0 or max_value / min_value < 1000:
        _, units = num_converter(max_value)
    else:
        _, units = num_converter(min_value)

    if units == "трлн.":
        factor = 1_000_000_000
    elif units == "млрд.":
        factor = 1_000_000
    elif units == "млн.":
        factor = 1_000
    else:
        factor = 1

    for i in range(len(values)):
        values[i] = values[i] / factor

    prev_export, curr_export, prev_import, curr_import = values

    prev_trade = prev_export + prev_import
    curr_trade = curr_export + curr_import

    prev_balance = prev_export - prev_import
    curr_balance = curr_export - curr_import

    def calc_growth(new, old, percent=True):
        if new == 0 and old == 0:
            return ""
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
        [f"{units} долл. США", f"{month_ranges[month]}{prev_year} год", f"{month_ranges[month]}{curr_year} год", f"Прирост {curr_year}/{prev_year}"],
        ["Товарооборот", f"{round(prev_trade, 1)}", f"{round(curr_trade, 1)}", calc_growth(curr_trade, prev_trade)],
        ["Экспорт", f"{round(prev_export, 1)}", f"{round(curr_export, 1)}", calc_growth(curr_export, prev_export)],
        ["Импорт", f"{round(prev_import, 1)}", f"{round(curr_import, 1)}", calc_growth(curr_import, prev_import)],
        ["Торговый баланс", f"{round(prev_balance, 1)}", f"{round(curr_balance, 1)}", format_balance_change(curr_balance, prev_balance)]
    ]

    return table


def convert_table(table_data):
    new_data = []

    max_value = max(float(table_data[0][2]), float(table_data[0][5]))

    if max_value > 100_000_000:
        units = "млрд."
        divider = 1_000_000
    elif max_value > 100_000:
        units = "млн."
        divider = 1_000
    else:
        units = "тыс."
        divider = 1

    for row in table_data:
        new_row = row.copy()
        new_row[2] = fn(round(float(row[2]) / divider, 2))
        new_row[5] = fn(round(float(row[5]) / divider, 2))
        new_data.append(new_row)

    return units, new_data

    