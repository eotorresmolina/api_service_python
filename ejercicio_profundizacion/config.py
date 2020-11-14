'''
Archivo de Configuración
---------------------------
Autor: Torres Molina Emmanuel O.
Version: 1.1
Descripcion:
Programa creado para modificar archivos de
configuración.
'''

__author__ = "Emmanuel Oscar Torres Molina"
__email__ = "emmaotm@gmail.com"
__version__ = "1.1"


from configparser import ConfigParser


def config(section, filename='config.ini'):
    parser = ConfigParser()
    parser.read(filename)

    config_param = {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config_param[param[0]]=param[1]

    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
 
    return config_param