#!/usr/bin/env python
'''
API Personas
---------------------------
Autor: Inove Coding School
Version: 1.0
 
Descripcion:
Se utiliza Flask para crear un WebServer que levanta los datos de
las personas registradas.

Ejecución: Lanzar el programa y abrir en un navegador la siguiente dirección URL
NOTA: Si es la primera vez que se lanza este programa crear la base de datos
entrando a la siguiente URL
http://127.0.0.1:5000/reset

Ingresar a la siguiente URL para ver los endpoints disponibles
http://127.0.0.1:5000/
'''

__author__ = "Inove Coding School"
__email__ = "INFO@INOVE.COM.AR"
__version__ = "1.0"

# Realizar HTTP POST --> https://www.codepunker.com/tools/http-requests

import traceback
import io
import sys
import os
import base64
import json
import sqlite3
from datetime import datetime, timedelta

import numpy as np
from flask import Flask, request, jsonify, render_template, Response, redirect
import matplotlib
matplotlib.use('Agg')   # For multi thread, non-interactive backend (avoid run in main loop)
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.image as mpimg

import persona
from config import config


app = Flask(__name__)

# Obtener la path de ejecución actual del script
script_path = os.path.dirname(os.path.realpath(__file__))

# Obtener los parámetros del archivo de configuración
config_path_name = os.path.join(script_path, 'config.ini')
db = config('db', config_path_name)
server = config('server', config_path_name)

persona.db = db


@app.route("/")
def index():
    try:
        # Imprimir los distintos endopoints disponibles
        result = "<h1>Bienvenido!!</h1>"
        result += "<h2>Endpoints disponibles:</h2>"
        result += "<h3>[GET] /reset --> borrar y crear la base de datos</h3>"
        result += "<h3>[GET] /personas?limit=[]&offset=[] --> mostrar el listado de personas (limite and offset are optional)</h3>"
        result += "<h3>[POST] /registro --> ingresar nuevo registro de pulsaciones por JSON</h3>"
        result += "<h3>[GET] /comparativa --> mostrar un gráfico que compare cuantas personas hay de cada nacionalidad"
        
        return(result)
    except:
        return jsonify({'trace': traceback.format_exc()})


@app.route("/reset")
def reset():
    try:
        # Borrar y crear la base de datos
        persona.create_schema()
        result = "<h3>Base de datos re-generada!</h3>"
        return (result)
    except:
        return jsonify({'trace': traceback.format_exc()})


@app.route("/personas")
def personas():
    try:
        # Mostrar todas las personas
        result = persona.report()
        return jsonify(result)
    except:
        return jsonify({'trace': traceback.format_exc()})


@app.route("/comparativa")
def comparativa():
    try:
        # Mostrar todos los registros en formato tabla
        """
        result = '''<h3>Implementar una función en persona.py
                    nationality_review</h3>'''
        result += '''<h3>Esa funcion debe devolver los datos que necesite
                    para implementar el grafico a mostrar</h3>'''
        return (result)
        """
        x, y = persona.nationality_review()

        fig, ax = plt.subplots(figsize=(16, 9))
        ax.set_title('Cantidad de Personas Según su Nacionalidad:', fontsize=15)
        ax.bar(x, y, color='darkgreen')
        ax.set_xlabel('Nationality', fontsize=16)
        ax.set_ylabel('Amount of People', fontsize=16)
        ax.set_facecolor('lightcyan')
        plt.grid('True')

        output = io.BytesIO()
        FigureCanvas(fig).print_png(output)
        plt.close(fig)

        return Response(output.getvalue(), mimetype='image/png')

    except:
        return jsonify({'trace': traceback.format_exc()})


@app.route("/registro", methods=['POST'])
def registro():
    if request.method == 'POST':
        # Obtener del HTTP POST JSON el nombre y los pulsos
        # name = ...
        # age = ...
        # nationality = ...
        
        # persona.insert(name, int(age), nationality)

        name = str(request.form.get('name'))
        age = str(request.form.get('age'))
        nationality = str(request.form.get('nationality'))

        persona.insert(name, int(age), nationality)

        return Response(status=200)
    

if __name__ == '__main__':
    print('Servidor arriba!')

    app.run(host=server['host'],
            port=server['port'],
            debug=True)
