#!/usr/bin/env python
'''
Heart DB manager
---------------------------
Autor: Inove Coding School
Version: 1.1

Descripcion:
Programa creado para administrar la base de datos de registro de personas
'''

__author__ = "Inove Coding School"
__email__ = "alumnos@inove.com.ar"
__version__ = "1.1"

import os
import sqlite3

db = {}


def create_schema():

    # Conectarnos a la base de datos
    # En caso de que no exista el archivo se genera
    # como una base de datos vacia
    conn = sqlite3.connect(db['database'])

    # Crear el cursor para poder ejecutar las querys
    c = conn.cursor()

    # Obtener el path real del archivo de schema
    script_path = os.path.dirname(os.path.realpath(__file__))
    schema_path_name = os.path.join(script_path, db['schema'])

    # Crar esquema desde archivo
    c.executescript(open(schema_path_name, "r").read())

    # Para salvar los cambios realizados en la DB debemos
    # ejecutar el commit, NO olvidarse de este paso!
    conn.commit()

    # Cerrar la conexión con la base de datos
    conn.close()


def insert(name, age, nationality):
    conn = sqlite3.connect(db['database'])
    c = conn.cursor()

    values = [name, age, nationality]

    c.execute("""
        INSERT INTO persona (name, age, nationality)
        VALUES (?,?,?);""", values)

    conn.commit()
    # Cerrar la conexión con la base de datos
    conn.close()


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def report(limit=0, offset=0):
    # Conectarse a la base de datos
    conn = sqlite3.connect(db['database'])
    c = conn.cursor()

    query = 'SELECT name, age, nationality FROM persona'

    if limit > 0:
        query += ' LIMIT {}'.format(limit)
        if offset > 0:
            query += ' OFFSET {}'.format(offset)

    query += ';'

    c.execute(query)
    query_results = c.fetchall()

    # Cerrar la conexión con la base de datos
    conn.close()
    return query_results


def nationality_review():
    nat = {}
    amount = 0
    data = report()
    d = [{'nationality': row[2]} for row in data]

    for pais in d:
        nationality = pais.get('nationality')       
        if nat.get(nationality) is not None:
            amount = nat.get(nationality) + 1
        else:
            amount = 1

        nat[nationality] = amount

    x = [key for key in nat.keys()]
    y = [value for value in nat.values()]

    return x, y


    
