import os
import time
import requests
import zipfile
import io
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


def get_kdg_data(region: str, sumbol: str, month: str, year: str) -> pd.DataFrame:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # чтобы не открывать окно браузера
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--log-level=3")
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 15)

    try:
        driver.get("https://kgd.gov.kz/ru/exp_trade_files")

        # Регион
        region_select_element = wait.until(EC.presence_of_element_located((By.ID, "edit-field-oblast-value")))
        Select(region_select_element).select_by_value(region)

        # Ждём, пока обновится выпадающий список "Тип"
        def type_dropdown_has_more_than_one_option(driver):
            options = driver.find_elements(By.CSS_SELECTOR, "#edit-field-type-value option")
            return len(options) > 1

        wait.until(type_dropdown_has_more_than_one_option)

        # Тип (фиксированный)
        type_select_element = driver.find_element(By.ID, "edit-field-type-value")
        Select(type_select_element).select_by_visible_text("Экспорт и импорт в разрезе товар-страна")

        # Знаки ТН ВЭД
        sumbol_select = Select(wait.until(EC.presence_of_element_located((By.ID, "edit-field-sumbol-value"))))
        sumbol_select.select_by_value(sumbol)

        # Период
        period_select = Select(wait.until(EC.presence_of_element_located((By.ID, "edit-field-period-value"))))
        period_select.select_by_value("0")  # Ежемесячно

        # Месяц
        month_select = Select(wait.until(EC.presence_of_element_located((By.ID, "edit-field-month-value"))))
        month_select.select_by_value(month)

        # Год
        year_input = wait.until(EC.presence_of_element_located((By.ID, "edit-field-year-value")))
        year_input.clear()
        year_input.send_keys(year)

        # Кнопка "Показать"
        show_button = wait.until(EC.element_to_be_clickable((By.ID, "showBtn")))
        show_button.click()

        # Ссылка "Скачать"
        download_link = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#exp_trade a")))
        file_url = download_link.get_attribute("href")
        if file_url.startswith(".."):
            file_url = "https://kgd.gov.kz" + file_url[2:]

        # Скачиваем и читаем Excel из ZIP
        response = requests.get(file_url)
        zip_bytes = io.BytesIO(response.content)
        with zipfile.ZipFile(zip_bytes, 'r') as zip_file:
            excel_filename = [name for name in zip_file.namelist() if name.endswith(('.xlsx', '.xls'))][0]
            with zip_file.open(excel_filename) as excel_file:
                df = pd.read_excel(excel_file)

        return df

    finally:
        driver.quit()



# print(download_trade_data("rk", "4z", "01", "2024"))