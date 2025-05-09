from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import requests
import rarfile
import io
import os
import time
import pandas as pd
from db.database import get_region_by_id, get_region_dic

# Указываем путь к unrar.exe, лежащему рядом с этим скриптом
rarfile.UNRAR_TOOL = os.path.join(os.path.dirname(__file__), "unrar.exe")

def get_statgov_data(region_code, kgd_code, digit, month, year):
    if kgd_code == "rk":
        kgd_code = "00"
    month = month.zfill(2)
    table_map = {4: 1, 6: 2, 10: 3}
    table_num = table_map.get(digit)

    # Настройка браузера
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--log-level=3")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    try:
        url = f"https://stat.gov.kz/ru/industries/economy/foreign-market/spreadsheets/?year={year}&name={region_code}&period=month&type=spreadsheets"
        driver.get(url)
        time.sleep(3)
        link = driver.find_element(By.CSS_SELECTOR, ".divTableCell.text-right a")
        file_url = link.get_attribute("href")
    finally:
        driver.quit()

    headers = {"User-Agent": "Mozilla/5.0"}
    rar_response = requests.get(file_url, headers=headers)
    rar_response.raise_for_status()

    rar_bytes = io.BytesIO(rar_response.content)
    with rarfile.RarFile(rar_bytes) as rf:
        # Определяем имя корневой папки
        first_folder = rf.namelist()[0].split('/')[0]
        # Собираем путь к нужному файлу
        target_file = f"{first_folder}/{month}/таб_{table_num}_{kgd_code}.xls"
        # print("📁 Ищем файл:", target_file)
        # print("📂 Содержимое архива:", rf.namelist())

        try:
            with rf.open(target_file) as tab_file:
                df = pd.read_excel(tab_file)
                return df
        except KeyError:
            print("❌ Файл не найден:", target_file)
        except Exception as e:
            print(f"⚠️ Ошибка при чтении файла: {e}")


# Пример вызова
# show_tab1_contents("07")
