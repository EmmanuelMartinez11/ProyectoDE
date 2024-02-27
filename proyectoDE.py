import requests
from configparser import ConfigParser
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
import pandas as pd
import json

#Parametros para la API
ids_cryptos = ["bitcoin", "bitcoin-cash", "cardano", "dogecoin", "eos", "ethereum", "iota", "stellar", "litecoin", "neo"]
vs_currency = "usd"
price_change_percentage = "1h,24h,7d"


#Funcion oara obtener los datos de la API
def obtenerDatosDeLaAPI(ids_cryptos, vs_currency, price_change_percentage):
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
    return data
datosDeLaAPI = obtenerDatosDeLaAPI(ids_cryptos,vs_currency,price_change_percentage)


# Función para obtener el string de conexión
def obtenerString(config_path, config_section):
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
    return urlConn
stringDeConexion = obtenerString("C:/Users/emmam/Desktop/proyecto/config/pipeline.conf", "RedShift")


#Creación del engine de SQLAlchemy
engine = sa.create_engine(stringDeConexion)


# Query para crear la tabla según los datos del JSON
# Se crea una columna id que es autoincrementable y es la primary key; y se crea una columna date que guarda las fechas
query = """
CREATE TABLE IF NOT EXISTS cryptos (
    id INT IDENTITY(1,1) PRIMARY KEY,
    id_crypto VARCHAR(50),
    symbol VARCHAR(10),
    name VARCHAR(50),
    current_price DECIMAL(20, 2),
    market_cap BIGINT,
    market_cap_rank INTEGER,
    fully_diluted_valuation BIGINT,
    total_volume BIGINT,
    high_24h DECIMAL(20, 2),
    low_24h DECIMAL(20, 2),
    price_change_24h DECIMAL(20, 2),
    price_change_percentage_24h DECIMAL(10, 5),
    market_cap_change_24h BIGINT,
    market_cap_change_percentage_24h DECIMAL(10, 5),
    circulating_supply BIGINT,
    total_supply BIGINT,
    max_supply BIGINT,
    ath DECIMAL(20, 2),
    ath_change_percentage DECIMAL(10, 5),
    ath_date TIMESTAMP,
    atl DECIMAL(20, 2),
    atl_change_percentage DECIMAL(20, 5),
    atl_date TIMESTAMP,
    last_updated TIMESTAMP,
    price_change_percentage_1h_in_currency DECIMAL(10, 5),
    price_change_percentage_24h_in_currency DECIMAL(10, 5),
    price_change_percentage_7d_in_currency DECIMAL(10, 5),
    date DATE
);
"""


# Ejecución de la query
with engine.connect() as connection:
    connection.execute(sa.text(query))


# Transformación del JSON a Dataframe
datosDeLaAPI = obtenerDatosDeLaAPI(ids_cryptos, vs_currency, price_change_percentage)
dataframe = pd.DataFrame(datosDeLaAPI)


# Transformaciones pertinentes al dataframe
# Se cambia el nombre de la columna id por id_crypto
# Se cargan los datos de la fecha usando la columna last_updated
# Se eliminan las columnas roi e image
dataframe.rename(columns={'id': 'id_crypto'}, inplace=True)
dataframe['date'] = dataframe['last_updated'].apply(lambda x: x.split('T')[0] if pd.notnull(x) else None)
dataframe = dataframe.drop(columns=['roi'])
dataframe = dataframe.drop(columns=['image'])


# Se sube el dataframe a la base de datos
# Se pone if_exists="append" para que, cuando se suban los nuevos valores, se posiciones siguientes a los anteiores valores
dataframe.to_sql(
    name="cryptos",
    con=engine,
    schema="emma_nionn_coderhouse",
    if_exists="append",
    method="multi",
    index=False,
)