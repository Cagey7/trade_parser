import re
import math
from datetime import datetime
import pandas as pd
from db.data.region_aliases import region_aliases
from db.database import insert_trade_data, get_country_dic, get_tn_ved_dic, get_region_dic, update_measure


CYRILLIC_LETTERS = [chr(code) for code in range(ord('А'), ord('Я') + 1)]

def insert_data(cur, df, month, year, region, format):
    tn_ved = ""
    country_dic = get_country_dic(cur)
    tn_ved_dic = get_tn_ved_dic(cur)
    region_dic = get_region_dic(cur)

    for index, row in df.iterrows():
        if is_tn_ved(row.iloc[0]):
            tn_ved = row.iloc[0]
            if none_if_nan(row.iloc[2]):
                update_measure(cur, row.iloc[0], row.iloc[2])
        if format == "импорт":
            import_tonn = row.iloc[3]
            import_additional = row.iloc[4]
            import_value = row.iloc[5]

            export_tonn = row.iloc[6]
            export_additional = row.iloc[7]
            export_value = row.iloc[8]
        elif format == "экспорт":
            export_tonn = row.iloc[3]
            export_additional = row.iloc[4]
            export_value = row.iloc[5]

            import_tonn = row.iloc[6]
            import_additional = row.iloc[7]
            import_value = row.iloc[8]

        try:
            country_name = row.iloc[1].upper()
            country_id = country_dic.get(country_name)
            if "��" in country_name:
                for letter in CYRILLIC_LETTERS:
                    candidate = country_name.replace("��", letter)
                    country_id = country_dic.get(candidate)
                    if country_id:
                        break
        except:
            continue

        tn_ved_id = tn_ved_dic.get(tn_ved)
        region_id = region_dic.get(region)

        if country_id and tn_ved:
            values = (
                none_if_nan(export_tonn),
                none_if_nan(export_additional),
                none_if_nan(export_value),
                none_if_nan(import_tonn),
                none_if_nan(import_additional),
                none_if_nan(import_value),
                country_id,
                region_id,
                tn_ved_id,
                year,
                month,
                datetime.now()
            )
            insert_trade_data(cur, values)


def get_data_frame(file_path):
    if file_path.endswith('.xls'):
        engine = 'xlrd'
    elif file_path.endswith('.xlsx'):
        engine = 'openpyxl'
    else:
        raise ValueError("Неподдерживаемый формат файла")

    df = pd.read_excel(file_path, engine=engine, header=None)
    return df


def get_month(df):
    header_info = ' '.join(map(str, df.head(5).values.flatten()))
    find_month = r'\b(январ[ья]|феврал[ья]|март[а]?|апрел[ья]|ма[йя]|июн[ья]|июл[ья]|август[а]?|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья])\b'
    months = re.findall(find_month, header_info, re.IGNORECASE)

    if months:
        month_str = months[0].lower()
        month_map = {
            'январь': 1, 'января': 1,
            'февраль': 2, 'февраля': 2,
            'март': 3, 'марта': 3,
            'апрель': 4, 'апреля': 4,
            'май': 5, 'мая': 5,
            'июнь': 6, 'июня': 6,
            'июль': 7, 'июля': 7,
            'август': 8, 'августа': 8,
            'сентябрь': 9, 'сентября': 9,
            'октябрь': 10, 'октября': 10,
            'ноябрь': 11, 'ноября': 11,
            'декабрь': 12, 'декабря': 12
        }
        month = month_map[month_str]
    else:
        num_monthes = re.findall(r'\b\d{2}\.\d{4}\b', header_info)
        month = int(num_monthes[1][:2])

    return month


def get_year(df):
    header_info = ' '.join(map(str, df.head(5).values.flatten()))
    find_year = r'\b(20[0-3][0-9]|2040)\b'
    years = re.findall(find_year, header_info)

    if years:
        year = int(years[0])
    else:
        return None
    return year


def get_region(df):
    header_info = ' '.join(map(str, df.head(5).values.flatten()))
    alias_to_name = {entry["alias"].lower(): entry["name"] for entry in region_aliases}

    pattern = r'\b(?:' + '|'.join(map(re.escape, alias_to_name.keys())) + r')\b'

    matches = []
    for match in re.finditer(pattern, header_info, flags=re.IGNORECASE):
        found_alias = match.group(0).lower()
        if found_alias in alias_to_name:
            matches.append(alias_to_name[found_alias])

    if "Республика Казахстан" in matches and len(matches) > 1:
        matches.remove("Республика Казахстан")

    return matches[0]


def get_digit(df):
    for index, row in df.iterrows():
        if is_tn_ved(row[0]):
            return len(row[0])

# Вспомогательные функции
def get_file_type(df):
    for index, row in df.iterrows():
        try:
            if row.iloc[3].strip().lower() == "импорт":
                return "импорт"
            if row.iloc[3].strip().lower() == "экспорт":
                return "экспорт"
        except:
            pass


def is_tn_ved(value):
    try:
        int(value)
        return True
    except (ValueError, TypeError):
        return False


def none_if_nan(x):
    return None if (isinstance(x, float) and math.isnan(x)) else x


def get_insert_info():
    data = []
    digits = [4,6]
    region_ids = list(range(2, 22))
    # region_ids = [1]


    for region_id in region_ids:
        for digit in digits:
            for month in range(1, 5):
                data.append({"region_id": region_id, "digit": digit, "month": month, "year": 2025})

            for month in range(1, 13):
                data.append({"region_id": region_id, "digit": digit, "month": month, "year": 2024})

            for month in range(1, 13):
                # if (region_id == 11 and digit == 4 and month in [2, 3, 4]) or (region_id == 15 and digit == 4 and month in [1]):
                #     continue
                data.append({"region_id": region_id, "digit": digit, "month": month, "year": 2023})
    return data