import numpy as np
import pickle
import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd
from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020')


def film_in_category(category_id: int) -> pd.DataFrame or None:
    """ Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego id kategorii.
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|

    Tabela wynikowa ma być posortowana po tylule filmu i języku.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    """
    if type(category_id) != int:
        return None
    df = pd.read_sql(f"SELECT film.title, language.name AS languge, category.name AS category FROM film \
                LEFT JOIN film_category ON film_category.film_id = film.film_id \
                LEFT JOIN category ON category.category_id = film_category.category_id \
                LEFT JOIN language ON language.language_id = film.language_id \
                WHERE category.category_id = {category_id} ORDER BY film.title, language.name ", con=connection)
    return df


def number_films_in_category(category_id: int) -> pd.DataFrame or None:
    """ Funkcja zwracająca wynik zapytania do bazy o ilość filmów w zadanej kategori przez id kategorii.
    Przykład wynikowej tabeli:
    |   |category   |count|
    |0	|Action 	|64	  |

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    """
    if type(category_id) != int:
        return None
    df = pd.read_sql(f"SELECT category.name AS category, COUNT(*) FROM film \
                LEFT JOIN film_category ON film_category.film_id = film.film_id \
                LEFT JOIN category ON category.category_id = film_category.category_id \
                WHERE category.category_id = {category_id} \
                GROUP BY category.name", con=connection)
    return df


def number_film_by_length(min_length: Union[int, float] = 0, max_length: Union[int, float] = 1e6) -> pd.DataFrame or None:
    """ Funkcja zwracająca wynik zapytania do bazy o ilość filmów o dla poszczegulnych długości pomiędzy wartościami min_length a max_length.
    Przykład wynikowej tabeli:
    |   |length     |count|
    |0	|46 	    |64	  |

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    min_length (int,float): wartość minimalnej długości filmu
    max_length (int,float): wartość maksymalnej długości filmu

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    """
    if type(min_length) not in [int, float] or type(max_length) not in [int, float] or max_length < min_length:
        return None
    df = pd.read_sql(f"SELECT film.length, COUNT(*) FROM film \
                WHERE film.length BETWEEN {min_length} AND {max_length} \
                GROUP BY film.length", con=connection)
    return df


def client_from_city(city: str) -> pd.DataFrame or None:
    """ Funkcja zwracająca wynik zapytania do bazy o listę klientów z zadanego miasta przez wartość city.
    Przykład wynikowej tabeli:
    |   |city	    |first_name	|last_name
    |0	|Athenai	|Linda	    |Williams

    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    city (str): nazwa miaste dla którego mamy sporządzić listę klientów

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    """
    if type(city) != str:
        return None
    df = pd.read_sql(f"SELECT city.city, customer.first_name, customer.last_name FROM customer \
                LEFT JOIN address ON address.address_id = customer.address_id \
                LEFT JOIN city ON city.city_id = address.city_id \
                WHERE city.city = '{city}' \
                ORDER BY customer.last_name, customer.first_name ", con=connection)
    return df


def avg_amount_by_length(length: Union[int, float]) -> pd.DataFrame or None:
    """ Funkcja zwracająca wynik zapytania do bazy o średnią wartość wypożyczenia filmów dla zadanej długości length.
    Przykład wynikowej tabeli:
    |   |length |avg
    |0	|48	    |4.295389


    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    length (int,float): długość filmu dla którego mamy pożyczyć średnią wartość wypożyczonych filmów

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    """
    if type(length) not in [int, float]:
        return None
    df = pd.read_sql(f"SELECT film.length, AVG(payment.amount) FROM film \
                LEFT JOIN inventory ON inventory.film_id = film.film_id \
                LEFT JOIN rental ON rental.inventory_id = inventory.inventory_id \
                LEFT JOIN payment ON payment.rental_id = rental.rental_id \
                WHERE film.length = {length} \
                GROUP BY film.length ", con=connection)
    return df


def client_by_sum_length(sum_min: Union[int, float]) -> pd.DataFrame or None:
    """ Funkcja zwracająca wynik zapytania do bazy o sumaryczny czas wypożyczonych filmów przez klientów powyżej zadanej wartości .
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |sum
    |0  |Brian	    |Wyman  	|1265

    Tabela wynikowa powinna być posortowane według sumy, imienia i nazwiska klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    sum_min (int,float): minimalna wartość sumy długości wypożyczonych filmów którą musi spełniać klient

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    """
    if type(sum_min) not in [int, float]:
        return None
    df = pd.read_sql(f"SELECT customer.first_name, customer.last_name, SUM(film.length) FROM customer \
                LEFT JOIN rental ON rental.customer_id = customer.customer_id \
                LEFT JOIN inventory ON inventory.inventory_id = rental.inventory_id \
                LEFT JOIN film ON film.film_id = inventory.film_id \
                GROUP BY customer.customer_id \
                HAVING SUM(film.length) >= {sum_min} \
                ORDER BY SUM(film.length), customer.last_name, customer.first_name ", con=connection)
    return df


def category_statistic_length(name: str) -> pd.DataFrame or None:
    """ Funkcja zwracająca wynik zapytania do bazy o statystykę długości filmów w kategorii o zadanej nazwie.
    Przykład wynikowej tabeli:
    |   |category   |avg    |sum    |min    |max
    |0	|Action 	|111.60 |7143   |47 	|185

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    name (str): Nazwa kategorii dla której ma zostać wypisana statystyka

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    """
    if type(name) != str:
        return None
    df = pd.read_sql(f"SELECT category.name AS category, AVG(film.length), SUM(film.length), MIN(film.length), MAX(film.length) FROM film \
                LEFT JOIN film_category ON film_category.film_id = film.film_id \
                LEFT JOIN category ON category.category_id = film_category.category_id \
                WHERE category.name = '{name}' \
                GROUP BY category.name ", con=connection)
    return df
