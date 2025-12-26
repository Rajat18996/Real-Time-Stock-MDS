WITH velocity_calc AS (
    SELECT
        symbol,
        market_timestamp,
        current_price,
        -- Get the price from 5 minutes ago (approx 50 rows if polling every 6 sec)
        LAG(current_price, 50) OVER (PARTITION BY symbol ORDER BY market_timestamp) as price_5min_ago
    FROM {{ ref('silver_clean_stock_quotes') }}
)
SELECT
    symbol,
    current_price,
    ((current_price - price_5min_ago) / NULLIF(price_5min_ago, 0)) * 100 AS velocity_pct_5min
FROM velocity_calc
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY market_timestamp DESC) = 1