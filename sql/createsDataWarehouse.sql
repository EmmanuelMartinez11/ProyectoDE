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
) DISTSTYLE EVEN
  SORTKEY (id_crypto);