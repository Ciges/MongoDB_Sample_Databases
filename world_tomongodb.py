#!/usr/bin/python
import pymysql
import pymongo

# Nos conectamos a la base de datos MySQL y a MongoDB
mysql_con = pymysql.connect('localhost', 'mongodb', 'mongodb', 'world')
mongodb_con = pymongo.MongoClient('mongodb://localhost/')
mongodb_db = mongodb_con.world

# Obtenemos la información de las ciudades
# ---------------------------------------------
cities = mysql_con.cursor()
cities.execute("select * from City")

cities_col = mongodb_db.City
cities_col.ensure_index('CountryCode')
for city in cities:
    # Creamos los documentos en la base de datos MongoDB
    city_doc = {
        '_id': city[0],
        'Name': city[1],
        'CountryCode': city[2],
        'District': city[3],
        'Population': city[4]
       }
    cities_col.insert(city_doc)
 
# Obtenemos la información de los países, incluyendo los lenguajes de cada uno como subdocumentos
# ---------------------------------------------
countries = mysql_con.cursor()
countries.execute("select * from Country")

countries_col = mongodb_db.Country
countries_col.ensure_index('Name', unique=True)
for country in countries:
    # Creamos el documento para cada país
    country_doc = {
        '_id': country[0],
        'Name': country[1],
        'Continent': country[2],
        'Region': country[3],
        'SurfaceArea': country[4],
        'IndepYear': country[5],
        'Population': country[6],
        'LifeExpectancy': country[7],
        'GNP': country[8],
        'GNPOld': country[9],
        'LocalName': country[10],
        'GovernmentForm': country[11],
        'HeadOfState': country[12],
        'Code2': country[14]
        }
    # Para la capital en vez del código de la ciudad ponemos el nombre, distrito y población
    if country[13] is not None:
        capital_cur = mysql_con.cursor()
        capital_cur.execute("select Name, District, Population from City where ID = " + str(country[13]))
        capital = capital_cur.fetchone()
        country_doc['Capital'] = {
            'Name': capital[0],
            'District': capital[1],
            'Population': capital[2]
            }
    # Buscamos sus idiomas
    languages = mysql_con.cursor()
    languages.execute("select * from CountryLanguage where CountryCode = '" + country[0] +"'")
    if languages.rowcount >= 1:
        country_doc['languages'] = []
        for language in languages:
            language_doc = {
                'Language': language[1],
                'Percentage': language[3]
                }
            if language[2] == 'T':
                language_doc['IsOfficial'] = True
            else:
                language_doc['IsOfficial'] = False
            country_doc['languages'].append(language_doc)
    
    countries_col.insert(country_doc)
