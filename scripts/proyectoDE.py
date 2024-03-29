import requests
from configparser import ConfigParser
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
import pandas as pd
import datetime
import psycopg2
import json
import smtplib
import logging

def obtenerString():
    #Funcion que devuelve el string de conexion (Antes estaba configurada con el .conf, pero no funciona)
    stringDeConexion = "redshift+redshift_connector://emma_nionn_coderhouse:2681OqlDQk@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database"
    return stringDeConexion


def crearEngineSA():
    #Función que crea el objeto conexion
    stringDeConexion = obtenerString()
    engine = sa.create_engine(stringDeConexion)
    return engine

def crearDataLake():
    #Función que crea el datalake
    engine = crearEngineSA()
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
                date TIMESTAMP
            ) DISTSTYLE EVEN
            """
    with engine.connect() as connection:
        connection.execute(sa.text(query))

def crearDataWarehouse():
    #Funcion que crea el datawarehouse
    engine = crearEngineSA()
    query = """
            CREATE TABLE IF NOT EXISTS cryptos (
                id INT IDENTITY(1,1) PRIMARY KEY,
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
                date TIMESTAMP
            ) DISTSTYLE EVEN
            SORTKEY (id);
            """
    with engine.connect() as connection:
        connection.execute(sa.text(query))


def obtenerDatosDeLaAPI(ids_cryptos, vs_currency, price_change_percentage):
    #Funcion para obtener los datos de una API
    cryptos = "%2C".join(ids_cryptos)
    urlAPI = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency={vs_currency}&ids={cryptos}&sparkline=false&price_change_percentage={price_change_percentage}"
    response = requests.get(urlAPI)
    data = response.json()
    return data

def cargarDataLake(ids_cryptos, vs_currency, price_change_percentage):
    #Funcion para cargar los datos de la API al datalake
    #Se realizan algunas transformaciones como pasar el roi a json para que se pueda subir a la tabla
    #Además se creó una columna date, para tener una mejor referencia de cuando se subieron los registros al datalake
    engine = crearEngineSA()
    datosDeLaAPI = obtenerDatosDeLaAPI(ids_cryptos, vs_currency, price_change_percentage)
    dataframe = pd.DataFrame(datosDeLaAPI)
    dataframe['roi'] = dataframe['roi'].apply(json.dumps)
    dataframe['date'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dataframe = dataframe.sort_values(by='name') 

    dataframe.to_sql(
        name="cryptos_data_lake",
        con=engine,
        schema="emma_nionn_coderhouse",
        if_exists="append",
        index=False,
    )

def extraerDatosDelDataLake():
    #Funcion que extrae las ultimas 10 cryptos subidas al datalake
    connPG2 = psycopg2.connect(
    dbname="data-engineer-database",
    user="emma_nionn_coderhouse",
    password="2681OqlDQk",
    host="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com",
    port="5439"
    )
    cur = connPG2.cursor()
    cur.execute("SELECT DISTINCT  * FROM emma_nionn_coderhouse.cryptos_data_lake ORDER BY date DESC LIMIT 10")
    column_names = [desc[0] for desc in cur.description]
    resultados = cur.fetchall()
    dataframe = pd.DataFrame(resultados, columns=column_names)
    return dataframe

def transformarDatos():
    #Funcion que realiza transformaciones sobre los datos extaidos del datalake
    dataframe = extraerDatosDelDataLake()
    dataframe = dataframe.drop(columns=['id', 'roi', 'image'])
    return dataframe


def cargarDataWarehouse():
    #Fucnion que sube los datos del datalake, transformados, al datawarehouse
    engine = crearEngineSA()
    dataframe = transformarDatos()
    dataframe.to_sql(
        name="cryptos",
        con=engine,
        schema="emma_nionn_coderhouse",
        if_exists="append",
        index=False,
        dtype={
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
            'date': sa.types.TIMESTAMP
        }
    )



def controlarCryptos(contrasenia):
    #Funcion que alerta cuando se cumple que: 
    #El volumen de las cryptos transaccionadas supere el 20%
    #El precio de las cryptos transaccionadas supere el 5%
    #El market cap (capitalización de mercado) de las cryptos transaccionadas supere el 10%


    connPG2 = psycopg2.connect(
            dbname="data-engineer-database",
            user="emma_nionn_coderhouse",
            password="2681OqlDQk",
            host="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com",
            port="5439"
        )
    cur = connPG2.cursor()
    cur.execute("SELECT * FROM emma_nionn_coderhouse.cryptos ORDER BY date DESC LIMIT 20") #Se toman 20 filas del datawarehouse
    column_names = [desc[0] for desc in cur.description]
    resultados = cur.fetchall()
    dataframe = pd.DataFrame(resultados, columns=column_names)
    dataframe = dataframe.sort_values(by='name') #Se ordenan por nombre para que se ubiquen en pares de cryptos
    #Ejemplo de los pares de cryptos: 
    #fila1: 19	btc	Bitcoin	70017.00    ...     2024-03-26 22:09:55.000
    #fila2: 5	btc	Bitcoin	70017.00    ...     2024-03-26 22:08:59.000

    #Arreglo que contiene el texto para enviar por mail
    mensajes_por_crypto = {}

    for i in range(0, len(dataframe), 2):
        crypto_nombre = dataframe.iloc[i]['name']

        #Calculo para comprar si una crypto superó el 20% de volumen
        volumen_fila_1 = dataframe.iloc[i]['total_volume']
        volumen_fila_2 = dataframe.iloc[i + 1]['total_volume']
        cambio_porcentaje_volumen = abs(volumen_fila_2 - volumen_fila_1) / volumen_fila_1 * 100

        #Calculo para comprar si una crypto superó el 5% de valor
        precio_fila_1 = dataframe.iloc[i]['current_price']
        precio_fila_2 = dataframe.iloc[i + 1]['current_price']
        cambio_porcentaje_precio = abs(precio_fila_2 - precio_fila_1) / precio_fila_1 * 100

        #Calculo para comprar si una crypto superó el 10% de market cap
        market_cap_fila_1 = dataframe.iloc[i]['market_cap']
        market_cap_fila_2 = dataframe.iloc[i + 1]['market_cap']
        cambio_porcentaje_cap_market = abs(market_cap_fila_2 - market_cap_fila_1) / market_cap_fila_1 * 100

        #Arreglo que contiene un mensaje especifico para cada crypto
        mensaje = []
        if cambio_porcentaje_volumen >= 20:
            mensaje.append(f"Aumento en un 20% el volumen de compras y ventas de la criptomoneda {crypto_nombre} en las ultimas 24 horas. ¡Apurate!")
        if cambio_porcentaje_precio >= 5:
            mensaje.append(f"El precio de la criptomoneda {crypto_nombre} ha experimentado un cambio significativo del {cambio_porcentaje_precio:.2f}% en las ultimas 24 horas.")
        if cambio_porcentaje_cap_market >= 10:
            mensaje.append(f"La capitalizacion de mercado de {crypto_nombre} ha aumentado mas del 10% en las ultimas 24 horas.")

        # Agrega el mensaje a la lista correspondiente de cada crypto
        if crypto_nombre in mensajes_por_crypto:
            mensajes_por_crypto[crypto_nombre].extend(mensaje)
        else:
            mensajes_por_crypto[crypto_nombre] = mensaje

    # Agrupa todos los mensajes en uno solo
    mensajes_agrupados = []
    for crypto, mensajes in mensajes_por_crypto.items():
        if mensajes:
            mensaje_agrupado = f"Noticias sobre {crypto}\n"
            mensaje_agrupado += "\n".join(mensajes)
            mensajes_agrupados.append(mensaje_agrupado)

    # Enviar un correo con todos los mensajes agrupados
    if mensajes_agrupados:
        try:
            x = smtplib.SMTP('smtp.gmail.com', 587)
            x.starttls()
            
            x.login('emma.nionn@gmail.com', contrasenia)

            asunto = 'Novedades crypto'
            cuerpoCorreo = "\n\n".join(mensajes_agrupados)
            message = 'Subject: {}\n\n{}'.format(asunto, cuerpoCorreo)
            message_ascii = message.encode('ascii', 'ignore')
            x.sendmail('emma.nionn@gmail.com', 'emma.nionn@gmail.com', message_ascii)

            logging.info(message_ascii)
        except Exception as exception:
            logging.error(f'Error al enviar correo electrónico: {exception}')
