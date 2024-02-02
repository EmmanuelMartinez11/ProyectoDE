
#### ProyectoDE
### ProyectoDE
## ProyectoDE
# ProyectoDE
#Este proyecto utiliza la API de CoinGecko para obtener información en tiempo real sobre criptomonedas y luego almacena esos datos en una base de datos Redshift utilizando SQLAlchemy.

## Archivo JSON
El script utiliza un JSON con datos de ejemplo de criptomonedas. Aquí se explica cada clave del JSON:
id_crypto: Identificador único de la criptomoneda.
symbol: Símbolo de la criptomoneda (por ejemplo, "btc" para Bitcoin).
name: Nombre completo de la criptomoneda.
image: URL de la imagen de la criptomoneda.
current_price: Precio actual de la criptomoneda en la moneda especificada.
market_cap: Capitalización de mercado de la criptomoneda.
market_cap_rank: Clasificación de la criptomoneda en términos de capitalización de mercado.
fully_diluted_valuation: Valoración totalmente diluida de la criptomoneda.
total_volume: Volumen total de operaciones en las últimas 24 horas.
high_24h: Precio más alto en las últimas 24 horas.
low_24h: Precio más bajo en las últimas 24 horas.
price_change_24h: Cambio de precio en las últimas 24 horas.
price_change_percentage_24h: Cambio porcentual de precio en las últimas 24 horas.
market_cap_change_24h: Cambio en la capitalización de mercado en las últimas 24 horas.
market_cap_change_percentage_24h: Cambio porcentual en la capitalización de mercado en las últimas 24 horas.
circulating_supply: Suministro circulante de la criptomoneda.
total_supply: Suministro total de la criptomoneda.
max_supply: Suministro máximo de la criptomoneda.
ath: Precio más alto histórico.
ath_change_percentage: Cambio porcentual desde el máximo histórico.
ath_date: Fecha del máximo histórico.
atl: Precio más bajo histórico.
atl_change_percentage: Cambio porcentual desde el mínimo histórico.
atl_date: Fecha del mínimo histórico.
roi: Retorno de la inversión (puede ser nulo).
last_updated: Fecha y hora de la última actualización de los datos.
price_change_percentage_1h_in_currency: Cambio porcentual de precio en la última hora en la moneda especificada.
price_change_percentage_24h_in_currency: Cambio porcentual de precio en las últimas 24 horas en la moneda especificada.
price_change_percentage_7d_in_currency: Cambio porcentual de precio en los últimos 7 días en la moneda especificada.

## Uso del Script
Configuración del Entorno:
Asegúrate de tener Python instalado en tu sistema.
Instala las bibliotecas necesarias ejecutando pip install requests sqlalchemy redshift_connector.

Ejecución del Script:
Clona el repositorio desde GitHub.
Ejecuta el script proyectoDE.py.
El script obtendrá datos de la API de CoinGecko y los almacenará en una base de datos Redshift.
Consideraciones sin pipeline.conf:

Dado que el archivo pipeline.conf no se comparte en el repositorio, se espera que los usuarios configuren manualmente la conexión a la base de datos.
Puedes modificar la función obtenerString para aceptar rutas relativas o utilizar variables de entorno para configurar la conexión de forma segura.

Configuración de Base de Datos:
Asegúrate de tener una base de datos Redshift disponible.
Modifica el script según sea necesario para adaptarlo a tu configuración específica de base de datos.
Notas Adicionales:

Puedes personalizar el conjunto de criptomonedas modificando la lista ids_cryptos en el script.
Consulta la documentación de CoinGecko para obtener más información sobre la API: CoinGecko API Documentation.
