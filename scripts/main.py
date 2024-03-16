import requests
from configparser import ConfigParser
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
import pandas as pd
import psycopg2



# Función para obtener el string de conexión
def crearEngineSA(config_path, config_section):
    """
    Construye la cadena de conexión a la base de datos a partir de un archivo de configuración.

    Parametros:
    config_path: ruta del archivo de configuración
    config_section: sección del archivo de configuración que contiene los datos de conexión a la base de datos

    Retorna:
    urlConn: cadena de conexión a la base de datos
    """
    parser = ConfigParser()    
    parser.read(config_path)

    config = parser[config_section]
    user = config['user']
    password = config['password']
    host = config['host']
    port = config['port']
    database = config['database']

    urlConn = URL.create(
        drivername='redshift+redshift_connector',
        host=host,
        port=port,
        database=database,
        username=user,
        password=password,
    )
    engine = sa.create_engine(urlConn)
    return engine


def obtenerDatosDeLaAPI(ids_cryptos, vs_currency, price_change_percentage, engine):
    """
    Retorna los datos de la API en formato JSON
    
    Parametros:
    ids_cryptos: identificador de las cryptos descritas en la API de coingecko
    vs_currency: moneda en la que van a estar expresados los valores de las cryptos
    price_change_percentage: variación, en porcentaje, de las criptos en el tiempo descripto 

    Retorna:
    data: datos de la API en formato JSON
    """
    cryptos = "%2C".join(ids_cryptos)
    urlAPI = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency={vs_currency}&ids={cryptos}&sparkline=false&price_change_percentage={price_change_percentage}"
    response = requests.get(urlAPI)
    data = response.json()
    dataframe = pd.DataFrame(data)
    dataframe = dataframe.drop(columns=['roi', 'image'])
    dataframe.to_sql
