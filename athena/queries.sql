
-- Athena Analytical Queries  (E-Commerce Real-Time Analytics Platform.


-- 1. Daily revenue and order volume (last 7 days).
SELECT
    order_date,
    COUNT(*)                         AS total_orders,
    SUM(total_amount)                AS gross_revenue,
    ROUND(AVG(total_amount), 2)      AS avg_order_value
FROM analytics_db.orders_processed
WHERE order_status = 'PLACED'
  AND order_date >= current_date - INTERVAL '7' DAY
GROUP BY order_date
ORDER BY order_date DESC;


-- 2. Revenue by product category (partition-pruned, fast).
SELECT
    category,
    COUNT(*)                         AS orders,
    SUM(total_amount)                AS revenue,
    ROUND(SUM(total_amount) * 100.0
          / SUM(SUM(total_amount)) OVER (), 2) AS pct_of_revenue
FROM analytics_db.orders_processed
WHERE order_status = 'PLACED'
GROUP BY category
ORDER BY revenue DESC;


-- 3. Top 10 products by revenue.
SELECT
    product_id,
    product_name,
    SUM(quantity)                    AS units_sold,
    SUM(total_amount)                AS revenue
FROM analytics_db.orders_processed
WHERE order_status = 'PLACED'
GROUP BY product_id, product_name
ORDER BY revenue DESC
LIMIT 10;


-- 4. Hourly order pattern (when do customers buy?).
SELECT
    order_hour,
    COUNT(*)                         AS orders,
    ROUND(AVG(total_amount), 2)      AS avg_order_value
FROM analytics_db.orders_processed
WHERE order_status = 'PLACED'
GROUP BY order_hour
ORDER BY order_hour;


-- 5. Cancellation rate by category (data-quality / business signal).
SELECT
    category,
    COUNT(*)                                                       AS total,
    SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END)    AS cancelled,
    ROUND(100.0 * SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END)
          / COUNT(*), 2)                                          AS cancel_rate_pct
FROM analytics_db.orders_processed
GROUP BY category
ORDER BY cancel_rate_pct DESC;


-- 6. Top cities by revenue.
SELECT
    shipping_state,
    shipping_city,
    COUNT(*)                         AS orders,
    SUM(total_amount)                AS revenue
FROM analytics_db.orders_processed
WHERE order_status = 'PLACED'
GROUP BY shipping_state, shipping_city
ORDER BY revenue DESC
LIMIT 15;


-- 7. Payment method mix.
SELECT
    payment_method,
    COUNT(*)                         AS orders,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM analytics_db.orders_processed
WHERE order_status = 'PLACED'
GROUP BY payment_method
ORDER BY orders DESC;
