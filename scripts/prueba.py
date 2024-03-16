import requests
from configparser import ConfigParser
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
import pandas as pd
import psycopg2
import json


# Parametros para la API
ids_cryptos = ["bitcoin", "bitcoin-cash", "cardano", "dogecoin", "eos", "ethereum", "iota", "stellar", "litecoin", "neo"]
vs_currency = "usd"
price_change_percentage = "1h,24h,7d"

# Funcion para obtener los datos de la API
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

# Creación del engine de SQLAlchemy
stringDeConexion = obtenerString("C:/Users/emmam/Desktop/proyectoDE/config/pipeline.conf", "RedShift")
engine = sa.create_engine(stringDeConexion)

query = """
CREATE TABLE IF NOT EXISTS cryptos_data_lake (
    id VARCHAR(50),
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
    roi VARCHAR(255),
    image VARCHAR(255),
    date DATE
) DISTSTYLE EVEN
  SORTKEY (id);

"""

with engine.connect() as connection:
    connection.execute(sa.text(query))

# Transformación del JSON a Dataframe
datosDeLaAPI = obtenerDatosDeLaAPI(ids_cryptos, vs_currency, price_change_percentage)
dataframe = pd.DataFrame(datosDeLaAPI)
dataframe['roi'] = dataframe['roi'].apply(json.dumps)

print(dataframe.columns)
dataframe.to_sql(
    name="cryptos_data_lake",
    con=engine,
    schema="emma_nionn_coderhouse",
    if_exists="append",
    index=False,
)


connPG2 = psycopg2.connect(
    dbname="data-engineer-database",
    user="emma_nionn_coderhouse",
    password="2681OqlDQk",
    host="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com",
    port="5439"
)
cur = connPG2.cursor()

# Ejecuta la consulta
cur.execute("SELECT * FROM emma_nionn_coderhouse.cryptos_data_lake ORDER BY last_updated DESC LIMIT 10")

# Obtener nombres de columnas
column_names = [desc[0] for desc in cur.description]

# Recupera todos los resultados
resultados = cur.fetchall()

# Crear DataFrame de Pandas
df = pd.DataFrame(resultados, columns=column_names)
df.rename(columns={'id': 'id_crypto'}, inplace=True)  # Renombrar la columna antes de la conversión
df['id_crypto'] = df['id_crypto'].astype(str)  # Convertir a cadena
df['last_updated'] = pd.to_datetime(df['last_updated'])
df['date'] = df['last_updated'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)
df = df.drop(columns=['roi', 'image'])

# Cierra el cursor y la conexión
cur.close()
connPG2.close()

df.to_sql(
    name="cryptos",
    con=engine,
    schema="emma_nionn_coderhouse",
    if_exists="append",
    index=False,
    dtype={
        'id': sa.types.Integer,
        'id_crypto': sa.types.String,
        'symbol': sa.types.String,
        'name': sa.types.String,
        'current_price': sa.types.DECIMAL,
        'market_cap': sa.types.BigInteger,
        'market_cap_rank': sa.types.Integer,
        'fully_diluted_valuation': sa.types.BigInteger,
        'total_volume': sa.types.BigInteger,
        'high_24h': sa.types.DECIMAL,
        'low_24h': sa.types.DECIMAL,
        'price_change_24h': sa.types.DECIMAL,
        'price_change_percentage_24h': sa.types.DECIMAL,
        'market_cap_change_24h': sa.types.BigInteger,
        'market_cap_change_percentage_24h': sa.types.DECIMAL,
        'circulating_supply': sa.types.BigInteger,
        'total_supply': sa.types.BigInteger,
        'max_supply': sa.types.BigInteger,
        'ath': sa.types.DECIMAL,
        'ath_change_percentage': sa.types.DECIMAL,
        'ath_date': sa.types.TIMESTAMP,
        'atl': sa.types.DECIMAL,
        'atl_change_percentage': sa.types.DECIMAL,
        'atl_date': sa.types.TIMESTAMP,
        'last_updated': sa.types.TIMESTAMP,
        'price_change_percentage_1h_in_currency': sa.types.DECIMAL,
        'price_change_percentage_24h_in_currency': sa.types.DECIMAL,
        'price_change_percentage_7d_in_currency': sa.types.DECIMAL,
        'date': sa.types.DATE
    }
)