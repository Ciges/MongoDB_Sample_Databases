#!/usr/bin/python
import pymysql
import pymongo

# Nos conectamos a la base de datos MySQL y a MongoDB
mysql_con = pymysql.connect('localhost', 'mongodb', 'mongodb', 'sakila')
mongodb_con = pymongo.MongoClient('mongodb://localhost/')
mongodb_db = mongodb_con.sakila

# Obtenemos la información de las películas
# ---------------------------------------------
# Cargamos en un array los idiomas (el id es autonumérico y empieza por 1)
language_cur = mysql_con.cursor()
language_cur.execute("select language_id,name from language order by language_id");
language = []
for element in language_cur:
    language.append(element[1]);

# Lo mismo para las categorías
category_cur = mysql_con.cursor()
category_cur.execute("select category_id,name from category order by category_id");
category = []
for element in category_cur:
    category.append(element[1]);


films = mysql_con.cursor()
films.execute("select * from film")

films_col = mongodb_db.films
for film in films:
    # Creamos un documento para cada película con toda la información
    film_doc = {
        '_id': film[0],
        'title': film[1],
        'description': film[2],
        'release_year': film[3],
        'rental_duration': film[6],
        'rental_rate': float(film[7]),
        'length': film[8],
        'replacement_cost': float(film[9]),
        'rating': film[10],
        'special_features': str(film[11])[1:-1].replace("'","").split(',')  # Get the string and transform it in a list of strings, one for each feature
        }
    if film[4] is not None:
        id_language = film[4]
        film_doc['language'] = language[id_language]
    if film[5] is not None:
        id_original_language = film[5]
        film_doc['original_language'] = language[id_original_language]
    # Buscamos los actores para cada película
    actors = mysql_con.cursor()
    actors.execute("select first_name,last_name from film_actor inner join actor on film_actor.actor_id = actor.actor_id where film_id = " + str(film[0]))
    if actors.rowcount >= 1:
        film_doc['actors'] = []
        for actor in actors:
            film_doc['actors'].append({
                'first_name': actor[0],
                'last_name': actor[1]
                })
    # Buscamos las categorías para cada película
    categories = mysql_con.cursor()
    categories.execute("select name from film_category inner join category on film_category.category_id = category.category_id where film_id = " + str(film[0]))
    if categories.rowcount >= 1:
        film_doc['categories'] = []
        for category in categories:
            film_doc['categories'].append(category[0])

    films_col.insert(film_doc)
# Añadimos índices en los arrays de actores y categorías
films_col.ensure_index('actors')
films_col.ensure_index('categories')
