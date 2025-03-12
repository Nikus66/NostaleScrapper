Łączy się z bazą danych – używa pyodbc i SQLAlchemy, by przechowywać informacje o kategoriach, subkategoriach i przedmiotach.
Konfiguruje Selenium WebDriver – otwiera stronę internetową z rynkiem przedmiotów, wybiera kategorie i subkategorie.
Scrapuje dane – pobiera nazwy, ilości, ceny i czas wygaśnięcia ofert przedmiotów.
Zapisuje dane do bazy – każda strona z przedmiotami jest analizowana i zapisywana do tabeli SQL.
Pracuje wielowątkowo – korzysta z ThreadPoolExecutor, by jednocześnie scrapować wiele kategorii i subkategorii, przyspieszając cały proces.
Podsumowując – to automatyczny bot zbierający dane o przedmiotach w NosTale, zapisujący je do bazy SQL dla dalszej analizy.
