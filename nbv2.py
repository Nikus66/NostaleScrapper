import pyodbc
import pandas as pd
from sqlalchemy import create_engine, VARCHAR, Integer, DateTime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
import time
from datetime import datetime
import configparser
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import os


# --- Konfiguracja ---
logging.basicConfig(
    filename='nostale_scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)

config = configparser.ConfigParser()
config.read('config.ini')

# Baza danych
server = config['Database']['server']
database = config['Database']['database']
username = config['Database']['username']
password = config['Database']['password']

# Strona internetowa
base_url = config['Website']['base_url']
server_name = config['Website']['server_name']
language = config['Website']['language']

# Połączenie z bazą danych
conn_str = (
    f'DRIVER={{ODBC Driver 18 for SQL Server}};'
    f'SERVER={server};DATABASE={database};'
    f'UID={username};PWD={password};'
    f'TrustServerCertificate=yes;'
)

try:
    conn = pyodbc.connect(conn_str)
    engine = create_engine('mssql+pyodbc:///?odbc_connect=' + conn_str)
    logging.info("Połączono z bazą danych.")
except Exception as e:
    logging.error(f"Błąd połączenia z bazą danych: {e}")
    exit()  # Zakończ skrypt, jeśli nie ma połączenia z DB

# --- Funkcje pomocnicze ---

def clean_quantity(quantity_str):
    if not isinstance(quantity_str, str):
        return None
    try:
        return int(quantity_str.replace(" ", ""))
    except ValueError:
        return None

def clean_price(price_str):
    if not isinstance(price_str, str):
        return None
    try:
        return int(price_str.replace(" ", "").replace("Gold", "").replace("szt.", "").replace(",", "").strip())
    except ValueError:
        return None


def create_tables(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                IF OBJECT_ID('categories', 'U') IS NULL
                CREATE TABLE categories (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(255) NOT NULL
                )
            """)
            cursor.execute("""
                IF OBJECT_ID('subcategories', 'U') IS NULL
                CREATE TABLE subcategories (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    category_id INT NOT NULL FOREIGN KEY REFERENCES categories(id),
                    name NVARCHAR(255) NOT NULL
                )
            """)
            cursor.execute("""
                IF OBJECT_ID('items', 'U') IS NULL
                CREATE TABLE items (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    CategoryID INT NOT NULL FOREIGN KEY REFERENCES categories(id),
                    CategoryName NVARCHAR(255) NOT NULL,
                    SubCategoryID INT NOT NULL FOREIGN KEY REFERENCES subcategories(id),
                    SubCategoryName NVARCHAR(255) NOT NULL,
                    Name NVARCHAR(255) NOT NULL,
                    Quantity INT,
                    Price INT,
                    TimeRemaining NVARCHAR(50),
                    DataScrapingu DATETIME NOT NULL
                )
            """)
            conn.commit()
            logging.info("Tabele utworzone (jeśli nie istniały).")
    except Exception as e:
        logging.error(f"Błąd podczas tworzenia tabel: {e}")
        exit()

def get_or_create_category_id(conn, category_name):
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM categories WHERE name = ?", category_name)
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                cursor.execute("INSERT INTO categories (name) OUTPUT INSERTED.id VALUES (?)", category_name)
                category_id = cursor.fetchone()[0]
                conn.commit()
                return category_id
    except Exception as e:
        logging.error(f"Błąd pobierania/tworzenia ID kategorii: {e}")
        return None


def get_or_create_subcategory_id(conn, category_id, subcategory_name):
     try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM subcategories WHERE name = ? AND category_id = ?", (subcategory_name, category_id))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                cursor.execute("INSERT INTO subcategories (category_id, name) OUTPUT INSERTED.id VALUES (?, ?)", (category_id, subcategory_name))
                subcategory_id = cursor.fetchone()[0]
                conn.commit()
                return subcategory_id
     except Exception as e:
        logging.error(f"Błąd pobierania/tworzenia ID subkategorii: {e}")
        return None

# --- Funkcje scrapujące i zapisujące ---

def scrape_page(driver, category_id, category_name, subcategory_id, subcategory_name):  # Bez data_queue
    """Scrapuje JEDNĄ stronę i zwraca dane (DataFrame) z TEJ STRONY."""
    data = []  # Lista na dane z *tej* strony
    logging.info(f"Oczekuję na elementy 'item' dla strony {driver.current_url}") # DODANE
    try:
        items = WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "item"))
        )
        logging.info(f"Znaleziono {len(items)} przedmiotów na stronie.")
    except TimeoutException:
        logging.warning("Nie znaleziono przedmiotów na stronie (Timeout).")
        return pd.DataFrame(data) # Zwracamy pusty DataFrame

    item_index = 0
    while item_index < len(items):
        try:
            # Pobierz element na nowo ZA KAŻDYM RAZEM
            item = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "item"))
            )[item_index]

            name = item.find_element(By.CSS_SELECTOR, 'button.all-searches-p').text
            quantity = item.find_element(By.XPATH, './/p[contains(@style, "left: 372px;")]').text
            price = item.find_element(By.XPATH, './/button[contains(@style, "left: 470px;")]').text
            time_remaining = item.find_element(By.XPATH, './/p[contains(@style, "left: 612px;")]').text

            data.append({
                'CategoryID': category_id,
                'CategoryName': category_name,
                'SubCategoryID': subcategory_id,
                'SubCategoryName': subcategory_name,
                'Name': name,
                'Quantity': clean_quantity(quantity),
                'Price': clean_price(price),
                'TimeRemaining': time_remaining,
                'DataScrapingu': datetime.now()
            })
            item_index += 1

        except NoSuchElementException:
            logging.warning(f"Nie znaleziono elementu w przedmiocie (indeks {item_index}). Pomijanie.")
            item_index += 1
        except StaleElementReferenceException:
            logging.warning(f"StaleElementReferenceException dla przedmiotu {item_index}. Ponawiam próbę.")
            time.sleep(0.25)
            continue
        except IndexError:
            logging.warning(f"IndexError - Prawdopodobnie strona się zmieniła. Kończę scrapowanie strony.")
            return  pd.DataFrame(data)
        except Exception as e:
            logging.exception(f"Nieoczekiwany błąd: {e}")
            return pd.DataFrame(data)
    return pd.DataFrame(data)  # Zwróć DataFrame z danymi z TEJ STRONY

def save_page_to_db(df, engine):
    """Zapisuje dane z JEDNEJ strony (DataFrame) do bazy danych."""
    if not df.empty:  # Sprawdź, czy DataFrame nie jest pusty
        try:
            df.to_sql('items', engine, if_exists='append', index=False,
                      dtype={
                          'CategoryID': Integer(),
                          'CategoryName': VARCHAR(255),
                          'SubCategoryID': Integer(),
                          'SubCategoryName': VARCHAR(255),
                          'Name': VARCHAR(255),
                          'Quantity': Integer(),
                          'Price': Integer(),
                          'TimeRemaining': VARCHAR(50),
                          'DataScrapingu': DateTime()
                      })
            logging.info(f"Zapisano {len(df)} rekordów do bazy danych.")
        except Exception as e:
            logging.error(f"Błąd zapisu do bazy danych: {e}")
    else:
        logging.warning("Brak danych do zapisania (pusty DataFrame).")


def scrape_subcategory(category_name, subcategory_name, category_value, subcategory_value, category_ids, subcategory_ids, engine):  # Dodaj engine
    """Scrapuje całą subkategorię, strona po stronie, i ZAPISUJE DANE PO KAŻDEJ STRONIE."""

    options = webdriver.FirefoxOptions()
    options.set_preference("permissions.default.image", 2)
    options.add_argument('--headless')

    with webdriver.Firefox(options=options) as driver:
        driver.implicitly_wait(30)  # ZWIĘKSZONO
        # Usun data = []

        try:
            driver.get(f"{base_url}?lang={language}&server={server_name}")
            WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.ID, "bibi-basar"))
            ).click()

            category_dropdown = Select(WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "categoryDropdown"))
            ))
            category_dropdown.select_by_value(category_value)

            subcategory_dropdown = Select(WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "subCategoryDropdown"))
            ))
            subcategory_dropdown.select_by_value(subcategory_value)

            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "search-button"))
            )
            search_button.click()
            time.sleep(2)
            search_button.click()
            time.sleep(2)

            category_id = category_ids[category_name]
            subcategory_id = subcategory_ids[(category_name, subcategory_name)]

            page_number = 1
            while True:
                logging.info(f"Scrapowanie strony {page_number} subkategorii {subcategory_name}.")
                page_data = scrape_page(driver, category_id, category_name, subcategory_id, subcategory_name) # Pobierz DataFrame z 1 strony
                logging.info(f"Próba zapisu danych dla strony: {driver.current_url}")# DODANE
                save_page_to_db(page_data, engine) # ZAPISZ DANE Z TEJ STRONY do bazy

                try:
                    next_button = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "button.pagination-button.next-button"))
                    )
                    next_button.click()
                    page_number += 1
                    time.sleep(0.8)
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, "item"))
                    )

                except TimeoutException:
                    logging.info(f"Osiągnięto ostatnią stronę subkategorii {subcategory_name}.")
                    break
                except NoSuchElementException:
                    logging.info(f"Nie znaleziono przycisku następnej strony subkategorii {subcategory_name}")
                    break

        except Exception as e:
            logging.exception(f"Błąd podczas scrapowania subkategorii {subcategory_name}: {e}")
            # Nie ma return []


# --- Konfiguracja kategorii i subkategorii ---

categories = {
    "Główny Przedmiot": "3310",
    "Przedmiot Konsumpcyjny": "3311"
}
subcategories = {
    "Główny Przedmiot": [
        ("Zwyczajne przedmioty", "3350"),
        ("Materiały do ulepszania", "3351"),
        ("Narzędzia", "3352"),  
        ("Przedmioty specjalne", "3353"),
        ("Eliksiry", "3354"),
        ("Event", "3355"),
        ("Tytuł", "4626")
    ],
    "Przedmiot Konsumpcyjny": [
        ("Składniki", "3359")
    ]
}

# --- Główna pętla ---

if __name__ == "__main__":
    create_tables(conn)

    category_ids = {}
    for category_name in categories:
        category_ids[category_name] = get_or_create_category_id(conn, category_name)

    subcategory_ids = {}
    for category_name, subcategory_list in subcategories.items():
        for subcategory_name, _ in subcategory_list:
            category_id = category_ids[category_name]
            subcategory_ids[(category_name, subcategory_name)] = get_or_create_subcategory_id(conn, category_id, subcategory_name)

    # Usun all_data

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for category_name, category_value in categories.items():
            for subcategory_name, subcategory_value in subcategories[category_name]:
                logging.info(f"Uruchamiam wątek dla: {category_name} - {subcategory_name}")
                future = executor.submit(scrape_subcategory, category_name, subcategory_name, category_value, subcategory_value, category_ids, subcategory_ids, engine)  # Przekazujemy engine
                futures.append(future)

        # Poczekaj na zakończenie wątków (ale nie zbieramy wyników)
        for future in as_completed(futures):
            try:
                future.result() #  wynik juz nie potrzebny
            except Exception as e:
                logging.error(f"Błąd w wątku: {e}")

    conn.close()
    logging.info("Skrypt zakończony.")