import mysql.connector
import requests
from bs4 import BeautifulSoup
from mysql.connector import Error

urlMov = "https://www.imdb.com/chart/top/?ref_=nv_mv_250"
urlTV = "https://www.imdb.com/chart/toptv/?ref_=nv_tvv_250"


# Scrapping IMDb
def scrap(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'lxml')
    data = []
    films = soup.find('tbody', class_='lister-list').findAll('tr')

    for film in films:
        link = "https://imdb.com" + film.find('td', class_='titleColumn').find('a').get('href')
        name = film.find('td', class_='titleColumn').find('a').text
        year = int(film.find('span', class_='secondaryInfo').text.replace("(", "").replace(")", ""))
        rate = float(film.find('td', class_='ratingColumn imdbRating').text.replace("\n", ""))
        data.append([name, year, rate, link])
    return data


# MySQL connection
def db_input(table_name, data):
    # Clear Database
    print("=== Start input in {table} ===".format(table=table_name))
    user_name = input("Enter username: ")
    passwd = input("Enter password: ")
    db_name = input("Choose database: ")
    try:
        con = mysql.connector.connect(user=user_name, password=passwd, host='127.0.0.1', database=db_name)
        cursor = con.cursor()
        cursor.execute("""TRUNCATE imdb.{table}""".format(table=table_name))
        con.commit()
        cursor.close()
        con.close()
    except Error as e:
        print(e)

    # Add items
    for movie in data:
        try:
            con = mysql.connector.connect(user=user_name, password=passwd, host='127.0.0.1', database=db_name)
            cursor = con.cursor()
            insert_query = """
                        INSERT INTO {table}
                        (name, year, rate, link)
                        VALUES ( %s, %s, %s, %s)
                    """.format(table=table_name)
            data_in = (movie[0], movie[1], movie[2], movie[3])
            cursor.execute(insert_query, data_in)

            con.commit()

            cursor.close()
            con.close()
        except Error as e:
            print(e)


# scrap(urlTV)
db_input('movies', scrap(urlMov))
db_input('tv', scrap(urlTV))
