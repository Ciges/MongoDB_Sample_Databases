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

# Obtenemos la información de los clientes
# ---------------------------------------------
customers = mysql_con.cursor()
customers.execute("select customer_id, first_name, last_name, email, address_id, active from customer")
customers_col = mongodb_db.customers
for customer in customers:
    # Creamos un documento para cada cliente, poniendo la información relativa a su dirección como subdocumentos
    customer_doc = {
        '_id': customer[0],
        'first_name': customer[1],
        'last_name': customer[2],
        'email': customer[3]
        }
    address_cur = mysql_con.cursor()
    address_cur.execute("select address, address2, district, postal_code, phone, city, country from address inner join city inner join country on address.city_id = city.city_id and city.country_id = country.country_id where address_id = " + str(customer[4]))
    address = address_cur.fetchone()
    customer_doc['address'] = {
        'address': address[0],
        'address2': address[1],
        'district': address[2],
        'postal_code': address[3],
        'phone': address[4],
        'city': address[5],
        'country': address[6]
        }
    if customer[5] == 1:
        customer_doc['active'] = True
    else:
        customer_doc['active'] = False

    customers_col.insert(customer_doc)

# Obtenemos la información de los DVD's (inventorio)
# ---------------------------------------------
inventory = mysql_con.cursor()
inventory.execute("select inventory_id,film_id from inventory")
inventory_col = mongodb_db.inventory
for dvd in inventory:
    # Creamos un documento para cada DVD del videoclub, incluyendo la información sobre las veces que ha sido alquilado
    dvd_doc = {
        '_id': dvd[0]
        }
    # Ponemos el título de la película
    film_cur = mysql_con.cursor()
    film_cur.execute("select title from film where film_id = " + str(dvd[1]))
    film = film_cur.fetchone()
    dvd_doc['title'] = film[0]
    # Añadimos los alquileres
    rentals = mysql_con.cursor()
    rentals.execute("select rental_date,return_date,first_name,last_name,phone from rental inner join customer inner join address on rental.customer_id = customer.customer_id and customer.address_id = address.address_id where inventory_id = " + str(dvd[0]))
    if rentals.rowcount >= 1:
        dvd_doc['rentals'] = []
        for rental in rentals:
            dvd_doc['rentals'].append({
                'rental_date': rental[0],
                'return_date': rental[1],
                'first_name': rental[2],
                'last_name': rental[3],
                'phone': rental[4]
                })
                
    inventory_col.insert(dvd_doc)
