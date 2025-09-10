import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, NVARCHAR, DateTime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, TimeoutException, \
    WebDriverException
import time
from datetime import datetime
import configparser
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.action_chains import ActionChains
import os

# --- Konfiguracja Logowania ---
logging.basicConfig(
    filename='nostale_scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)

config = configparser.ConfigParser()
config.read('config.ini')

# --- Konfiguracja Bazy Danych ---
server = config['Database']['server']
database = config['Database']['database']
username = config['Database']['username']
password = config['Database']['password']

# --- Konfiguracja Strony Internetowej ---
base_url = config['Website']['base_url']
server_name = config['Website']['server_name']
language = config['Website']['language']

# --- Połączenie z Bazą Danych ---
conn_str = (
    f'DRIVER={{ODBC Driver 18 for SQL Server}};'
    f'SERVER={server};DATABASE={database};'
    f'UID={username};PWD={password};'
    f'TrustServerCertificate=yes;'
)

try:
    conn = pyodbc.connect(conn_str)
    engine = create_engine('mssql+pyodbc:///?odbc_connect=' + conn_str,
                           fast_executemany=True,
                           pool_size=10, max_overflow=20, pool_timeout=30, pool_recycle=3600
                           )
    logging.info("Połączono z bazą danych.")
except Exception as e:
    logging.error(f"Błąd połączenia z bazą danych: {e}")
    exit()

# Lista realistycznych User-Agentów (możesz dodać więcej)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.2478.67",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0"
]

# --- Lista dostępnych eventów ---
events_list = [
    "Brak Eventu",
    "Ulepszenie Ekwipunku",
    "Podwójny Drop",
    "Podwójne Doświadczenie",
    "Ulepszanie Karty Specjalisty",
    "Podwójne Pudełka Rajdu",
    "Podwójne złoto",
    "Ulepszanie Run",
    "Ulepszanie Tatuaży",
    "Ulepszanie Kart Specjalisty Partnera",
    "Podwójne Błyskawiczne Bitwy"
]


# Definicje funkcji do czyszczenia danych
def clean_quantity(quantity_str):
    """Usuwa spacje i konwertuje ilość na liczbę całkowitą."""
    if not isinstance(quantity_str, str):
        logging.warning(f"Oczekiwano stringa dla ilości, otrzymano: {type(quantity_str)}, wartość: {quantity_str}")
        return None
    try:
        return int(quantity_str.replace(" ", ""))
    except ValueError:
        logging.warning(f"Nie można przekonwertować ilości na liczbę: {quantity_str}")
        return None


def clean_price(price_str):
    """Usuwa spacje, 'Gold', 'szt.' i WSZYSTKIE przecinki, konwertuje cenę na liczbę całkowitą."""
    if not isinstance(price_str, str):
        logging.warning(f"Oczekiwano stringa dla ceny, otrzymano: {type(price_str)}, wartość: {price_str}")
        return None
    try:
        price_cleaned = price_str.replace(" ", "").replace("Gold", "").replace("szt.", "").replace(",", "").strip()
        return int(price_cleaned)
    except ValueError:
        logging.warning(f"Nie można przekonwertować ceny na liczbę: {price_str}")
        return None


# Funkcja do "ludzkiego" kliknięcia
def human_click(driver, element):
    """Przewija do elementu, symuluje ruch myszy i kliknięcie."""
    try:
        actions = ActionChains(driver)

        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        time.sleep(random.uniform(0.05, 0.15))

        actions.move_to_element(element)
        time.sleep(random.uniform(0.05, 0.1))

        offset_x = random.randint(-3, 3)
        offset_y = random.randint(-3, 3)
        actions.move_by_offset(offset_x, offset_y).click().perform()
        time.sleep(random.uniform(0.1, 0.3))
    except Exception as e:
        logging.warning(f"Błąd podczas symulacji ludzkiego kliknięcia: {e}. Próbuję standardowego kliknięcia.")
        element.click()
        time.sleep(random.uniform(0.05, 0.1))


def scrape_items_from_page(driver, category_ids, category_name, subcategory_ids, subcategory_name, current_event_name):
    """
    Scrauje przedmioty z aktualnie załadowanej strony i ZWRACA listę zebranych słowników.
    """
    data_page_items = []
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "item"))
        )

        items = driver.find_elements(By.CLASS_NAME, 'item')

        if not items:
            return []

        item_index = 0
        while item_index < len(items):
            retry = 0
            success = False
            while not success and retry < 3:
                items = driver.find_elements(By.CLASS_NAME, 'item')
                if item_index >= len(items):
                    logging.warning(
                        f"Subkategoria: {subcategory_name} - Próba dostępu do przedmiotu {item_index} poza zakresem po odświeżeniu listy. Przechodzę do następnego.")
                    break

                item = items[item_index]

                try:
                    name_element = WebDriverWait(item, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'button.all-searches-p')))
                    quantity_element = WebDriverWait(item, 5).until(
                        EC.presence_of_element_located((By.XPATH, './/p[contains(@style, "left: 372px;")]')))
                    price_element = WebDriverWait(item, 5).until(
                        EC.presence_of_element_located((By.XPATH, './/button[contains(@style, "left: 470px;")]')))
                    time_remaining_element = WebDriverWait(item, 5).until(
                        EC.presence_of_element_located((By.XPATH, './/p[contains(@style, "left: 612px;")]')))

                    name = name_element.text
                    quantity = quantity_element.text
                    price = price_element.text
                    time_remaining = time_remaining_element.text

                    data_page_items.append({
                        'CategoryID': category_ids[category_name],
                        'CategoryName': category_name,
                        'SubCategoryID': subcategory_ids[(category_name, subcategory_name)],
                        'SubCategoryName': subcategory_name,
                        'Name': name,
                        'Quantity': clean_quantity(quantity),
                        'Price': clean_price(price),
                        'TimeRemaining': time_remaining,
                        'DataScrapingu': datetime.now(),
                        'Event': current_event_name
                    })
                    success = True
                    break
                except StaleElementReferenceException as e:
                    retry += 1
                    logging.warning(
                        f"Subkategoria: {subcategory_name}, Przedmiot {item_index} - StaleElementReferenceException (próba {retry}). Ponawiam próbę odczytu.")
                    time.sleep(random.uniform(1.0, 2.0))
                except (NoSuchElementException, TimeoutException) as e:
                    logging.error(
                        f"Subkategoria: {subcategory_name}, Przedmiot {item_index} - Błąd: element nie znaleziono lub timeout (wewnętrzny). Przechodzę do następnego.")
                    break
                except Exception as e:
                    logging.exception(
                        f"Subkategoria: {subcategory_name}, Przedmiot {item_index} - Nieoczekiwany błąd podczas przetwarzania elementu. Przechodzę do następnego.")
                    break

            if not success:
                logging.error(
                    f"Subkategoria: {subcategory_name}, Przedmiot {item_index} - Nie udało się pobrać danych po kilku próbach.")
            item_index += 1

        return data_page_items

    except TimeoutException as e:
        logging.warning(
            f"Subkategoria: {subcategory_name} - Timeout podczas oczekiwania na listę przedmiotów na stronie: {e}. (Może być pusta)")
        return []
    except WebDriverException as e:
        logging.error(
            f"Subkategoria: {subcategory_name} - Błąd WebDrivera podczas scrapowania przedmiotów ze strony: {e}")
        return []
    except Exception as e:
        logging.exception(
            f"Subkategoria: {subcategory_name} - Nieoczekiwany błąd podczas scrapowania przedmiotów ze strony: {e}")
        return []


def scrape_subcategory_data(category_name, subcategory_name, category_value, subcategory_value, category_ids,
                            subcategory_ids, base_url, current_event_name):
    """
    Scrauje dane dla pojedynczej subkategorii i zapisuje je bezpośrednio do bazy danych.
    """
    driver = None
    try:
        chrome_options = ChromeOptions()

        selected_user_agent = random.choice(USER_AGENTS)
        chrome_options.add_argument(f"user-agent={selected_user_agent}")

        chrome_options.add_argument("--start-maximized")
        # chrome_options.add_argument("--headless") # Pamiętaj, żeby to włączyć, gdy skończysz debugować!

        chrome_options.add_argument("--lang=pl-PL")

        # --- Dodatkowe opcje Chrome mogące pomóc ---
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--disable-notifications')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--window-size=1400,900')

        # --- Tworzenie instancji przeglądarki z automatycznym wykrywaniem wersji ---
        # USUWAMY parametr 'version_main'
        driver = uc.Chrome(options=chrome_options)

        # --- Wzmocnienie skryptów anty-detekcyjnych ---
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """
        })
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                window.chrome = {
                    runtime: {},
                    app: {},
                    csi: () => {},
                    loadTimes: () => {}
                };
            """
        })
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [
                        { description: 'Portable Document Format', filename: 'internal-pdf-viewer', name: 'PDF Viewer', ... },
                        { description: 'Shockwave Flash', filename: 'internal-nacl-plugin', name: 'Shockwave Flash', ... }
                    ],
                });
            """
        })
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['pl-PL', 'pl', 'en-US', 'en'],
                });
            """
        })

        driver.implicitly_wait(5)

        # --- KLUCZOWE MIEJSCE DLA CLOUDFLARE INITIAL CHALLENGE ---
        driver.get(base_url)
        logging.info(f"Wątek: {subcategory_name} - Ładuję stronę główną. Oczekuję na Cloudflare.")
        time.sleep(random.uniform(10, 25))  # Opóźnienie dla Cloudflare

        bibi_basar = WebDriverWait(driver, 20).until(  # Zwiększono timeout, bo to zaraz po CF
            EC.element_to_be_clickable((By.ID, "bibi-basar"))
        )
        human_click(driver, bibi_basar)
        time.sleep(random.uniform(0.5, 1.0))

        category_dropdown_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "categoryDropdown"))
        )
        category_dropdown = Select(category_dropdown_element)
        human_click(driver, category_dropdown_element)
        category_dropdown.select_by_value(category_value)
        time.sleep(random.uniform(0.5, 1.0))

        subcategory_dropdown_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "subCategoryDropdown"))
        )
        subcategory_dropdown = Select(subcategory_dropdown_element)
        human_click(driver, subcategory_dropdown_element)
        subcategory_dropdown.select_by_value(subcategory_value)
        time.sleep(random.uniform(0.5, 1.0))

        search_button = driver.find_element(By.CLASS_NAME, "search-button")

        human_click(driver, search_button)
        logging.info(f"Wątek: {subcategory_name} - Wybrano '{category_name}'/'{subcategory_name}'. Klikam 'Szukaj'.")
        time.sleep(random.uniform(3, 6))

        human_click(driver, search_button)

        WebDriverWait(driver, 25).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "item"))
        )
        logging.info(
            f"Wątek: {subcategory_name} - Przedmioty załadowane po wyszukiwaniu. Rozpoczynam scrapowanie strony 1.")
        time.sleep(random.uniform(0.5, 1.5))

        current_page = 1
        while True:
            data_current_page = scrape_items_from_page(driver, category_ids, category_name, subcategory_ids,
                                                       subcategory_name, current_event_name)

            if data_current_page:
                df_current_page = pd.DataFrame(data_current_page)
                try:
                    df_current_page.to_sql('items', engine, if_exists='append', index=False, dtype={
                        'CategoryID': Integer(),
                        'CategoryName': NVARCHAR(255),
                        'SubCategoryID': Integer(),
                        'SubCategoryName': NVARCHAR(255),
                        'Name': NVARCHAR(255),
                        'Quantity': Integer(),
                        'Price': Integer(),
                        'TimeRemaining': NVARCHAR(50),
                        'DataScrapingu': DateTime(),
                        'Event': NVARCHAR(255)
                    }, chunksize=1000)
                    logging.info(
                        f"Wątek: {subcategory_name} - Strona {current_page} zapisana do bazy danych ({len(df_current_page)} rekordów).")
                except Exception as db_error:
                    logging.error(
                        f"Wątek: {subcategory_name} - Błąd zapisu Strony {current_page} do bazy danych: {db_error}")
            else:
                logging.info(
                    f"Wątek: {subcategory_name} - Strona {current_page} nie zawiera przedmiotów. Koniec paginacji dla tej subkategorii.")
                break

            try:
                next_page_button = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, "button.pagination-button.next-button:not([disabled])"))
                )

                old_item = None
                try:
                    old_item = driver.find_element(By.CLASS_NAME, "item")
                except NoSuchElementException:
                    pass

                human_click(driver, next_page_button)
                logging.info(f"Wątek: {subcategory_name} - Przechodzę do Strony {current_page + 1}.")

                if old_item:
                    WebDriverWait(driver, 20).until(EC.staleness_of(old_item))

                WebDriverWait(driver, 30).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "item"))
                )

                current_page += 1
                time.sleep(random.uniform(0.5, 1.0))

            except (NoSuchElementException, TimeoutException):
                logging.info(
                    f"Wątek: {subcategory_name} - Brak kolejnych stron dla subkategorii '{subcategory_name}'. Zakończono paginację.")
                break
            except WebDriverException as e:
                logging.error(
                    f"Wątek: {subcategory_name} - Krytyczny błąd WebDrivera podczas nawigacji na następną stronę dla '{subcategory_name}': {e}. Wątek nie powiódł się.")
                return False
            except Exception as e:
                logging.exception(
                    f"Wątek: {subcategory_name} - Nieoczekiwany błąd podczas nawigacji na następną stronę dla '{subcategory_name}': {e}. Wątek nie powiódł się.")
                return False

        logging.info(f"Wątek: {subcategory_name} - Zakończono scrapowanie subkategorii '{subcategory_name}'.")
        return True

    except TimeoutException as e:
        logging.warning(
            f"Wątek: {subcategory_name} - Timeout podczas inicjalizacji/nawigacji dla '{subcategory_name}': {e}. Wątek nie powiódł się.")
        return False
    except WebDriverException as e:
        logging.error(
            f"Wątek: {subcategory_name} - Błąd WebDrivera podczas inicjalizacji/podstawowych operacji dla '{subcategory_name}': {e}. Wątek nie powiódł się.")
        return False
    except Exception as e:
        logging.exception(
            f"Wątek: {subcategory_name} - Nieoczekiwany błąd w wątku subkategorii '{subcategory_name}': {e}. Wątek nie powiódł się.")
        return False
    finally:
        if driver:
            try:
                driver.quit()
                logging.info(f"Wątek: {subcategory_name} - Przeglądarka zamknięta dla '{subcategory_name}'.")
            except Exception as e:
                logging.error(
                    f"Wątek: {subcategory_name} - Błąd podczas zamykania przeglądarki dla '{subcategory_name}': {e}")


# --- Sekcja wyboru eventu przed rozpoczęciem scrapowania ---
print("\nWybierz aktualny event:")
for i, event in enumerate(events_list):
    print(f"{i + 1}. {event}")

selected_event_index = -1
while not (0 < selected_event_index <= len(events_list)):
    try:
        user_input = input(f"Podaj numer eventu (1-{len(events_list)}): ")
        selected_event_index = int(user_input)
        if not (0 < selected_event_index <= len(events_list)):
            print("Nieprawidłowy numer. Spróbuj ponownie.")
    except ValueError:
        print("Nieprawidłowy numer. Wprowadź liczbę.")

current_event_name = events_list[selected_event_index - 1]
print(f"\nWybrano event: {current_event_name}\n")
logging.info(f"Rozpoczynanie scrapowania z wybranym eventem: {current_event_name}")

# Tworzenie tabel w bazie danych (jeśli nie istnieją)
with conn.cursor() as cursor:
    cursor.execute("""
    IF OBJECT_ID('categories', 'U') IS NULL
    CREATE TABLE categories (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(255) NOT NULL
    )
    """)
    conn.commit()

with conn.cursor() as cursor:
    cursor.execute("""
    IF OBJECT_ID('subcategories', 'U') IS NULL
    CREATE TABLE subcategories (
        id INT IDENTITY(1,1) PRIMARY KEY,
        category_id INT NOT NULL FOREIGN KEY REFERENCES categories(id),
        name NVARCHAR(255) NOT NULL
        )
    """)
    conn.commit()

with conn.cursor() as cursor:
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

# Zmiana: Dodanie kolumny 'Event' do tabeli 'items' jeśli jej nie ma
with conn.cursor() as cursor:
    try:
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                       WHERE TABLE_SCHEMA = 'dbo' 
                       AND TABLE_NAME = 'items' 
                       AND COLUMN_NAME = 'Event')
        BEGIN
            ALTER TABLE items ADD Event NVARCHAR(255);
        END
        """)
        conn.commit()
        logging.info("Sprawdzono i ewentualnie dodano kolumnę 'Event' do tabeli 'items'.")
    except Exception as e:
        logging.error(f"Błąd podczas dodawania kolumny 'Event' do tabeli 'items': {e}")

categories = {
    "Główny Przedmiot": "3310",
    "Przedmiot Konsumpcyjny": "3311"
}

subcategories = {
    "Główny Przedmiot": [
        #("Zwyczajne przedmioty", "3350"),
       # ("Materiały do ulepszania", "3351"),
       # ("Narzędzia", "3352"),
        ("Przedmioty specjalne", "3353")
      #  ("Eliksiry", "3354"),
      # ("Event", "3355"),
       # ("Tytuł", "4626")
    ],
    "Przedmiot Konsumpcyjny": [
        ("Składniki", "3359")
    ]
}

category_ids = {}
for category_name in categories.keys():
    query = f"SELECT id FROM categories WHERE name = ?"
    with conn.cursor() as cursor:
        cursor.execute(query, category_name)
        result = cursor.fetchone()
        if not result:
            query_insert = f"INSERT INTO categories (name) OUTPUT INSERTED.id VALUES (?)"
            cursor.execute(query_insert, category_name)
            category_id = cursor.fetchone()[0]
            category_ids[category_name] = category_id
            conn.commit()
        else:
            category_ids[category_name] = result[0]

subcategory_ids = {}
for category_name, subcategories_list in subcategories.items():
    for subcategory_name, _ in subcategories_list:
        query_check = f"SELECT id FROM subcategories WHERE name = ? AND category_id = ?"
        with conn.cursor() as cursor:
            cursor.execute(query_check, (subcategory_name, category_ids[category_name]))
            result = cursor.fetchone()
        if result:
            subcategory_id = result[0]
        else:
            query_insert = f"INSERT INTO subcategories (category_id, name) OUTPUT INSERTED.id VALUES (?, ?)"
            with conn.cursor() as cursor:
                cursor.execute(query_insert, (category_ids[category_name], subcategory_name))
                subcategory_id = cursor.fetchone()[0]
                conn.commit()
        subcategory_ids[(category_name, subcategory_name)] = subcategory_id

base_url = f"{base_url}?lang={language}&server={server_name}"

# Utrzymujemy bardzo duże opóźnienie między wątkami (dla WinError 183)
MIN_THREAD_START_DELAY = 15.0  # Duże opóźnienie
MAX_THREAD_START_DELAY = 30.0  # Bardzo duże opóźnienie

with ThreadPoolExecutor(max_workers=1) as executor:
    futures = []
    first_thread_started = False
    for category_name, category_value in categories.items():
        for subcategory_name, subcategory_value in subcategories.get(category_name, []):
            if first_thread_started:
                delay = random.uniform(MIN_THREAD_START_DELAY, MAX_THREAD_START_DELAY)
                logging.info(f"Oczekiwanie na uruchomienie kolejnego wątku przez {delay:.2f} sekundy...")
                time.sleep(delay)
            else:
                first_thread_started = True

            logging.info(f"Uruchamianie wątku dla subkategorii: {subcategory_name}")
            future = executor.submit(scrape_subcategory_data, category_name, subcategory_name, category_value,
                                     subcategory_value, category_ids, subcategory_ids, base_url, current_event_name)
            futures.append(future)

    for future in as_completed(futures):
        try:
            thread_status = future.result()
            if thread_status:
                logging.info(f"Wątek zakończył pracę pomyślnie.")
            else:
                logging.warning("Wątek zakończony z błędami.")

        except Exception as e:
            logging.error(f"Błąd pobierania wyników z wątku: {e}")

logging.info("Zakończono wszystkie wątki scrapujące.")
print("Skrypt zakończony.")