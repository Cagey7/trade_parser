import re
from db.data.month_ranges import month_ranges

def fn(x):
    return f"{x:,.1f}".replace(",", " ").replace(".0", "")


def gen_decline_growth_row(data, trend, units_of_account):
    if data[-1] < 0:
        name = re.sub(r'^\d+\.\s*', '', data[0].split(' (код')[0]).strip().lower()
        curr_value = float(data[2])
        prev_value = float(data[5])
        drop_value = abs(data[-1])
        if trend == "decline":
            if prev_value == 0:
                return
            drop_percent = drop_value / prev_value * 100
            return f"{name} - на {round(drop_percent, 1)}% или на {fn(drop_value)} {units_of_account}. долл. США (с {fn(prev_value)} до {fn(curr_value)} {units_of_account}. долл. США)"
        elif trend == "growth":
            growth_value = data[-2]
            if growth_value.startswith("+"):
                growth_value = f"на {growth_value[1:]}"
            elif growth_value == "new":
                growth_value = "на 100%"

            return f"{name} - {growth_value} или на {fn(drop_value)} {units_of_account}. долл. США (с {fn(curr_value)} до {fn(prev_value)} {units_of_account}. долл. США)"


def gen_summary_row(data, units_of_account):
    name = re.sub(r'^\d+\.\s*', '', data[0].split(' (код')[0]).strip().lower()
    value = float(data[5])
    share = data[6].strip()
    return f"{name} – {fn(value)} {units_of_account}. долл. США (с долей {share})"



def gen_summary_text(flow_data, country, month, year, region, units_of_account):
    direction, prev_value, current_value, change = flow_data
    prev_value = float(prev_value)
    current_value = float(current_value)
    if direction == "Экспорт":
        if change == 'new':
            return f"{direction} из {region} в {country} за {month_ranges[month]} {year} году начался и составил {current_value:.1f} {units_of_account}. долл. США."
        elif '-' in change and '%' in change:
            return f"{direction} из {region} в {country} за {month_ranges[month]} {year} год снизился на {change.strip('-')} и составил {current_value:.1f} {units_of_account}. долл. США."
        elif '+' in change:
            return f"{direction} из {region} в {country} за {month_ranges[month]} {year} год вырос на {change.strip('+')} и составил {current_value:.1f} {units_of_account}. долл. США."
        elif 'рост в' in change:
            return f"{direction} из {region} в {country} за {month_ranges[month]} {year} год увеличился {change} и составил {current_value:.1f} {units_of_account}. долл. США."
        else:
            return f"{direction} из {region} в {country} за {month_ranges[month]} {year} год составил {current_value:.1f} {units_of_account}. долл. США."
    elif direction == "Импорт":
        if change == 'new':
            return f"{direction} в {region} из {country} за {year} году начался и составил {current_value:.1f} {units_of_account}. долл. США."
        elif '-' in change and '%' in change:
            return f"{direction} в {region} из {country} за {month_ranges[month]} {year} год снизился на {change.strip('-')} и составил {current_value:.1f} {units_of_account}. долл. США."
        elif '+' in change:
            return f"{direction} в {region} из {country} за {month_ranges[month]} {year} год вырос на {change.strip('+')} и составил {current_value:.1f} {units_of_account}. долл. США."
        elif 'рост в' in change:
            return f"{direction} в {region} из {country} за {month_ranges[month]} {year} год увеличился {change} и составил {current_value:.1f} {units_of_account}. долл. США."
        else:
            return f"{direction} в {region} из {country} за {month_ranges[month]} {year} год составил {current_value:.1f} {units_of_account}. долл. США."
    elif direction == "Товарооборот":
        if change == 'new':
            return f"{direction} между {region} и {country} за {month_ranges[month]} {year} году начался и составил {current_value:.1f} {units_of_account}. долл. США."
        elif '-' in change and '%' in change:
            return f"{direction} между {region} и {country} за {month_ranges[month]} {year} год снизился на {change.strip('-')} и составил {current_value:.1f} {units_of_account}. долл. США."
        elif '+' in change:
            return f"{direction} между {region} и {country} за {month_ranges[month]} {year} год вырос на {change.strip('+')} и составил {current_value:.1f} {units_of_account}. долл. США."
        elif 'рост в' in change:
            return f"{direction} между {region} и {country} за {month_ranges[month]} {year} год увеличился {change} и составил {current_value:.1f} {units_of_account}. долл. США."
        else:
            return f"{direction} между {region} и {country} за {month_ranges[month]} {year} год составил {current_value:.1f} {units_of_account} долл. США."



def gen_text_flow(data, data_rev, flow_data, country, year, region, month, flow_type, units_of_account):

    if flow_type == "export":
        export_text = []

        summary_text = gen_summary_text(flow_data[2], country, month, year, region, units_of_account)
        export_text.append(summary_text)

        decline_data = sorted(data_rev[1:], key=lambda x: x[-1], reverse=False)

        rows_decline_text = []
        for row in decline_data[:7]:
            result = gen_decline_growth_row(row, 'decline', units_of_account)
            if result is not None:
                rows_decline_text.append(result)
        decline_text = (
            f"Сокращение экспорта в {country} обосновывается снижением поставок таких товаров, как: "
            f"{', '.join(rows_decline_text)}"
        )
        export_text.append(decline_text)

        growth_data = sorted(data[1:], key=lambda x: x[-1], reverse=False)
        
        row_growth_text = []
        for row in growth_data[:7]:
            result = gen_decline_growth_row(row, 'growth', units_of_account)
            if result is not None:
                row_growth_text.append(result)
        growth_text = (
            f"Вместе с тем, наблюдается рост поставок таких товаров, как: "
            f"{', '.join(row_growth_text)}"
        )
        export_text.append(growth_text)
        
        row_main_text = []
        for row in data[1:8]:
            result = gen_summary_row(row, units_of_account)
            if result is not None:
                row_main_text.append(result)

        main_text = (
            f"Основными товарами экспорта в {region} из {country} являются: "
            f"{', '.join(row_main_text)}"
        )

        export_text.append(main_text)

        info_text = f"Более подробная информация по основным экспортируемым товарам в {country} за {month_ranges[month]} {year} год показана в Таблице №1."
        export_text.append(info_text)

        return export_text
    
    if flow_type == "import":
        import_text = []
        summary_text = gen_summary_text(flow_data[3], country, month, year, region, units_of_account)
    
        import_text.append(summary_text)

        decline_data = sorted(data_rev[1:], key=lambda x: x[-1], reverse=False)
        row_decline_text = []
        for row in decline_data[:7]:
            result = gen_decline_growth_row(row, 'decline', units_of_account)
            if result is not None:
                row_decline_text.append(result)

        decline_text = (
            f"Сокращение импорта из {country} обосновывается снижением ввоза таких товаров, как: "
            f"{', '.join(row_decline_text)}"
        )
        import_text.append(decline_text)
        
        growth_data = sorted(data[1:], key=lambda x: x[-1], reverse=False)
        row_growth_text = []
        for row in growth_data[:7]:
            result = gen_decline_growth_row(row, 'growth', units_of_account)
            if result is not None:
                row_growth_text.append(result)

        growth_text = (
            f"Вместе с тем, наблюдается рост импорта таких товаров, как: "
            f"{', '.join(row_growth_text)}"
        )
        import_text.append(growth_text)

        row_main_text = []
        for row in data[1:8]:
            result = gen_summary_row(row, units_of_account)
            if result is not None:
                row_main_text.append(result)

        main_text = (
            f"Основными товарами импорта в {region} из {country} являются: "
            f"{', '.join(row_main_text)}"
        )
        import_text.append(main_text)

        info_text = f"Более подробная информация по основным импортируемым товарам из {country} за {month_ranges[month]} {year} год показана в Таблице №2."
        import_text.append(info_text)

        return import_text


def import_text_gen():
    pass