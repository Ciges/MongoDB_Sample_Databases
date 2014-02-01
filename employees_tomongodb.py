#!/usr/bin/python
import pymysql
import pymongo
import datetime

# Nos conectamos a la base de datos MySQL y a MongoDB
mysql_con = pymysql.connect('localhost', 'mongodb', 'mongodb', 'employees')
mysql_cur = mysql_con.cursor()
mongodb_con = pymongo.MongoClient('mongodb://localhost/')
mongodb_db = mongodb_con.employees

# Funciones
# ---------------------------------------------
# Devuelve el nombre de un departamento dado su id
def get_departmentName(id):
    return departmentName[id]

# Devuelve un objeto datetime a partir de un objeto datetime.date
def get_datetime(date):
    return datetime.datetime(date.year, date.month, date.day, 0, 0)

# Obtenemos la información de los departamentos
# ---------------------------------------------
mysql_cur.execute("select * from departments")

departments_col = mongodb_db.departments
departments_col.ensure_index('dept_name', unique=True)
departmentName = {}
for department in mysql_cur:
    # Creamos los documentos en la base de datos MongoDB
    dept_no = department[0]
    if departments_col.find_one({"_id": dept_no}) is None:
        department_doc = {
            '_id': dept_no,
            'dept_name': department[1]
            }
        departments_col.insert(department_doc)
    # Ponemos los datos en un diccionario para poder consultarlos posteriormente
    departmentName[dept_no] = department[1]

# Recorremos los empleados, actualizando la información de los departamentos cuando proceda
# ---------------------------------------------
mysql_cur.execute("select * from employees")

n = 0
employees_col = mongodb_db.employees
for employee in mysql_cur:
    employee_doc = {
        '_id': employee[0],
        'birth_date': get_datetime(employee[1]),
        'first_name': employee[2],
        'last_name': employee[3],
        'gender': employee[4],
        'hire_date': get_datetime(employee[5]),
        }

    # Para cada empleado recorremos el histórico de salarios
    salaries = mysql_con.cursor()
    salaries.execute("select * from salaries where emp_no = " + str(employee_doc['_id']))
    for salary in salaries:
        employee_doc['salaries_history'] = []
        # Si el año de la fecha actual es 9999 entonces es el salario actual
        if salary[3].year == 9999:
            employee_doc['salary'] = salary[1]
            employee_doc['salaries_history'].append({
                'salary': salary[1],
                'from_date': get_datetime(salary[2]),
                })
        else:
             employee_doc['salaries_history'].append({
                'salary': salary[1],
                'from_date': get_datetime(salary[2]),
                'to_date': get_datetime(salary[3])
                })
            
        
    # Para cada empleado recorremos el histórico de cargos
    titles = mysql_con.cursor()
    titles.execute("select * from titles where emp_no = " + str(employee_doc['_id']))
    for title in titles:
        employee_doc['title'] = []
        employee_doc['titles_history'] = []
        # Si el año de la fecha actual es 9999 entonces es el cargo actual. ¡Puede tener más de un cargo!
        if title[3].year == 9999:
            employee_doc['title'].append(title[1])
            employee_doc['titles_history'].append({
                'title': title[1],
                'from_date': get_datetime(title[2]),
                })
        else:
            employee_doc['titles_history'].append({
                'title': title[1],
                'from_date': get_datetime(title[2]),
                'to_date': get_datetime(title[3])
                })
         
    # Para cada empleado recorremos el histórico de sus departamentos
    depts_emp = mysql_con.cursor()
    depts_emp.execute("select * from dept_emp where emp_no = " + str(employee_doc['_id']))
    for dept_emp in depts_emp:
        employee_doc['department'] = []
        employee_doc['departments_history'] = []
        # Almacenamos el nombre del departamento, no el número
        department_name = get_departmentName(dept_emp[1])
        # Si el año de la fecha actual es 9999 entonces es el departamento actual. ¡Puede pertenecer a más de un departamento!
        if dept_emp[3].year == 9999:
            employee_doc['department'].append(department_name)
            employee_doc['departments_history'].append({
                'department': department_name,
                'from_date': get_datetime(dept_emp[2]),
                })
            # Actualizamos el número de empleados para el departamento correspondiente
            departments_col.update({"_id": dept_emp[1]}, { "$inc" : { "number_employees" : 1 } })
        else:
            employee_doc['departments_history'].append({
                'department': department_name,
                'from_date': get_datetime(dept_emp[2]),
                'to_date': get_datetime(dept_emp[3])
                })
 
    # Para cada empleado recorremos el histórico de los departamentos de los que ha sido jefe
    depts_manager = mysql_con.cursor()
    depts_manager.execute("select * from dept_manager where emp_no = " + str(employee_doc['_id']))
    for dept_manager in depts_manager:
        employee_doc['manager'] = []
        employee_doc['managers_history'] = []
        # Almacenamos el nombre del departamento, no el número
        department_name = get_departmentName(dept_manager[0])
        # Si el año de la fecha actual es 9999 entonces es el departamento actual. ¡Puede pertenecer a más de un departamento!
        if dept_manager[3].year == 9999:
            employee_doc['manager'].append(department_name)
            employee_doc['managers_history'].append({
                'department': department_name,
                'from_date': get_datetime(dept_manager[2]),
                })
            # Añadimos la información del jefe al departamento
            departments_col.update({"_id": dept_manager[1]}, { "$set" : { "manager" : {
                'emp_no': employee_doc['_id'],
                'first_name': employee_doc['first_name'],
                'last_name': employee_doc['last_name'],
                }           
            } })
        else:
             employee_doc['managers_history'].append({
                'department': department_name,
                'from_date': get_datetime(dept_manager[2]),
                'to_date': get_datetime(dept_manager[3])
                })

    # Añadimos el documento a la colección de empleados
    employees_col.save(employee_doc)
  
    # Cada 100 empleados procesados imprimimos un mensaje informativo
    n += 1
    if n % 100 == 0:
        print("Número de empleados procesados: " + str(n))


mysql_cur.close()
mysql_con.close()
mongodb_con.close()
