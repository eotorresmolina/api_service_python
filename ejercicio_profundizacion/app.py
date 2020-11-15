'''
Ejercicio de Profundización

API Titulos Completados

---------------------------
Autor: Torres Molina Emmanuel O.
Version: 1.1
Descripción:
Programa creado para que el alumno ponga sus habilidades JSON, 
matplotlib, bases de datos, request API JSON y Rest API.
'''

__author__ = "Emmanuel Oscar Torres Molina"
__email__ = "emmaotm@gmail.com"
__version__ = "1.1"


import sqlite3
import requests
import traceback
import os
import io
import json
from flask import Flask, request, Response, jsonify, render_template
from config import config

import matplotlib
matplotlib.use('Agg')
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib import pyplot as plt
from matplotlib.figure import Figure


script_path = os.path.dirname(os.path.realpath(__file__))
config_path_name = os.path.join(script_path, 'config.ini')

db = config('db', config_path_name)


def create_schema():
    schema_path_name = os.path.join(script_path, db['schema'])

    conn = sqlite3.connect(db['database'])
    c = conn.cursor()

    c.executescript(open(schema_path_name, 'r').read())

    conn.commit()
    conn.close()


def clear( ):
    conn = sqlite3.connect(db['database'])
    c = conn.cursor()

    c.execute("""DELETE FROM course;""")
    conn.commit()
    conn.close()


def fetch():
    url = config('dataset', filename=config_path_name).get('url')
    response = requests.get(url)
    data = response.json()
    return data


def insert_group(group):
    conn = sqlite3.connect(db['database'])
    c = conn.cursor()

    c.executemany("""
                    INSERT INTO course (id, userId, title, completed)
                    VALUES (?, ?, ?, ?);
                """, group)

    conn.commit()
    conn.close()


def show_table_SQL():
    conn = sqlite3.connect(db['database'])
    c = conn.cursor()

    query = """
            SELECT *
            FROM course AS c;
            """
    
    c.execute(query)
    
    print('\n(id, userId, title, completed)')

    for row in c:
        print(row)

    conn.close()

    print('\n\n')


def fill(chunk_activate=False, chunksize=10):
    data = fetch()
    group = [(row['id'], row['userId'], row['title'], row['completed']) 
                for row in data]

    if chunk_activate is True:
        chunk=[]
        for item in group:
            chunk.append(item)
            if len(chunk) == chunksize:
                insert_group(group=chunk)
                chunk.clear()
        if chunk:
            insert_group(group=chunk)

    else:
        insert_group(group=group)


def title_completed_count(userId=1):
    conn = sqlite3.connect(db['database'])
    c = conn.cursor()

    c.execute(""" SELECT COUNT(id) AS count_id
                FROM course
                WHERE userId = ? AND completed = 1;
            """, (userId, ))

    count = c.fetchone()
    conn.close()
    
    return count[0]


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def show(dict_form=False):
    conn = sqlite3.connect(db['database'])

    if dict_form is True:
        conn.row_factory = dict_factory

    c = conn.cursor()

    query = """
            SELECT c.id, c.userId, c.title, c.completed
            FROM course AS c;
            """

    c.execute(query)
    query_result = c.fetchall()

    conn.close()
    return query_result


def get_amount_user():
    conn = sqlite3.connect(db['database'])
    c = conn.cursor()

    query = """SELECT MAX(c.userId) AS max_userId
                FROM course AS c;
            """
    
    c.execute(query)
    query_result = c.fetchone()

    conn.close()

    return query_result[0]


def report():
    d = {}
    max_number_userId = get_amount_user()
    
    for user in range(1, max_number_userId + 1):
        d[user] = title_completed_count(userId=user)

    x = [element for element in d.keys()]
    y = [element for element in d.values()]

    return x, y


# API:

# Creo el Server.
app = Flask(__name__)

endpoint = config('endpoints', config_path_name) # Obtengo los endpoints
server = config('server', config_path_name) # Obtengo los datos del servidor

# Ruta que se Ingresar por la URL: 127.0.0.1:5000/
@app.route(endpoint['index'])
def index():
    try:
        result = '<h1>Bienvenido!!</h1>'
        result += '<h2>Endpoints Disponibles:</h2>'
        result += '<h3>[GET] /user --> Muestra el total de Usuarios</h3>'
        result += '<h3>[GET] /user/{id}/titles --> Mostrar cantidad de títulos completados por el Usuario</h3>'
        result += '<h3>[GET] /user/graph --> Mostrar Gráfico Comparativo de los Usuarios</h3>'
        result += '<h3>[GET] /user/table --> Mostrar Títulos Completados por Usuarios'

        return (result)

    except:
        return jsonify({'trace:': traceback.format_exc()})


# Ruta que se Ingresar por la URL: 127.0.0.1:5000/user
@app.route(endpoint['user'])
def user():
    try:
        data = show(dict_form=True)
        return (jsonify(data))
    
    except:
        return jsonify({'trace': traceback.format_exc()})


# Ruta que se Ingresar por la URL: 127.0.0.1:5000/user/{id}/titles
@app.route(endpoint['user_id_titles'])
def user_id_titles(id):
    try:
        count = title_completed_count(userId=id)
        return jsonify({'userId': id, 'title_completed_count': count})

    except:
        return jsonify({'trace': traceback.format_exc()})


# Ruta que se Ingresar por la URL: 127.0.0.1:5000/user/graph
@app.route(endpoint['user_graph'])
def user_graph():
    try:
        x, y = report()

        # Realizo el Gráfico de Barras que deseo mostrar:
        fig, ax = plt.subplots(figsize=(16, 9))
        ax.set_title('Number of Titles Completed by Users:', fontsize=18)
        ax.bar(x, y, color='darkblue')
        ax.set_xlabel('UserId', fontsize=15)
        ax.set_ylabel('Number of Titles', fontsize=15)
        ax.set_facecolor('whitesmoke')
    
        output = io.BytesIO()
        FigureCanvas(fig).print_png(output)
        plt.close(fig)

        return Response(output.getvalue(), mimetype='image/png')

    except:
        return jsonify({'trace': traceback.format_exc()})


# Ruta que se Ingresar por la URL: 127.0.0.1:5000/user/table
@app.route(endpoint['user_table'])
def user_table():
    try:
        tabla_html = config('template', config_path_name)

        x, y = report()

        return render_template(tabla_html['tabla'], row=zip(x, y))

    except:
        return jsonify({'trace': traceback.format_exc()})

    


if __name__ == "__main__":

    # Crear DB:
    create_schema()
    
    # Borrar DB:
    clear()

    fill(chunk_activate=True, chunksize=15)

    # Muestro la Tabla SQL Completa:
    show_table_SQL()

    count = title_completed_count(userId=2)
    print('\n\n{}\n\n'.format(count))

    print('Servidor Corriendo!!\n')

    # Lanzo el Servidor:
    app.run(host=server['host'], port=server['port'], debug=True)



