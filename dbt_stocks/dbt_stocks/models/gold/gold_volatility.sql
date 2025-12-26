SELECT
    symbol,
    AVG(current_price) OVER (PARTITION BY symbol ORDER BY market_timestamp ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) as moving_avg_20,
    STDDEV(current_price) OVER (PARTITION BY symbol ORDER BY market_timestamp ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) as std_dev_20,
    (moving_avg_20 + (2 * std_dev_20)) as upper_band,
    (moving_avg_20 - (2 * std_dev_20)) as lower_band
FROM {{ ref('silver_clean_stock_quotes') }}