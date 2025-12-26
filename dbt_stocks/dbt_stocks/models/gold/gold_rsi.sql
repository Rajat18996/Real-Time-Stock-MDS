WITH price_diffs AS (
    SELECT
        symbol,
        market_timestamp,
        current_price,
        -- Calculate change from previous record using LAG
        current_price - LAG(current_price) OVER (PARTITION BY symbol ORDER BY market_timestamp) AS diff
    FROM {{ ref('silver_clean_stock_quotes') }}
),

gains_losses AS (
    SELECT
        *,
        CASE WHEN diff > 0 THEN diff ELSE 0 END AS gain,
        CASE WHEN diff < 0 THEN ABS(diff) ELSE 0 END AS loss
    FROM price_diffs
),

avg_gl AS (
    SELECT
        symbol,
        market_timestamp,
        -- Standard RSI uses a 14-period lookback
        AVG(gain) OVER (PARTITION BY symbol ORDER BY market_timestamp ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS avg_gain,
        AVG(loss) OVER (PARTITION BY symbol ORDER BY market_timestamp ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS avg_loss
    FROM gains_losses
),

rsi_calc AS (
    SELECT
        symbol,
        market_timestamp,
        -- Handle division by zero if avg_loss is 0
        CASE
            WHEN avg_loss = 0 THEN 100
            ELSE 100 - (100 / (1 + (avg_gain / NULLIF(avg_loss, 0))))
        END AS rsi
    FROM avg_gl
)

-- Final output: Latest RSI value per symbol
SELECT
    symbol,
    rsi,
    market_timestamp AS last_updated,
    CASE
        WHEN rsi >= 70 THEN 'Overbought'
        WHEN rsi <= 30 THEN 'Oversold'
        ELSE 'Neutral'
    END AS market_signal
FROM rsi_calc
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY market_timestamp DESC) = 1