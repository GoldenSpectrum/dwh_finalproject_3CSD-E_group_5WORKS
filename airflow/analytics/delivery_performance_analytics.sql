-- =====================================================
-- DELIVERY PERFORMANCE ANALYTICS
-- Uses: fact_order_delays + fact_orders + dim_date + dim_merchant
-- =====================================================

-- -----------------------------------------------------
-- 4.1 Percentage of delayed orders
-- -----------------------------------------------------
CREATE OR REPLACE VIEW pct_delayed_orders AS
SELECT
    ROUND(
        COUNT(*) FILTER (WHERE delay_in_days > 0)::numeric
        / NULLIF(COUNT(*), 0) * 100,
        2
    ) AS pct_delayed_orders
FROM fact_order_delays;


-- -----------------------------------------------------
-- 4.2 Merchants with most delayed orders
-- -----------------------------------------------------
CREATE OR REPLACE VIEW delayed_orders_by_merchant AS
SELECT
    m.merchant_name,
    COUNT(*) AS delayed_orders
FROM fact_order_delays d
JOIN fact_orders o
    ON d.order_id = o.order_id
JOIN dim_merchant m
    ON o.merchant_sk = m.merchant_sk
    AND m.is_current = true
WHERE d.delay_in_days > 0
GROUP BY m.merchant_name
ORDER BY delayed_orders DESC;


-- -----------------------------------------------------
-- 4.3 Average delay days per merchant
-- -----------------------------------------------------
CREATE OR REPLACE VIEW avg_delay_per_merchant AS
SELECT
    m.merchant_name,
    AVG(d.delay_in_days) AS avg_delay_days
FROM fact_order_delays d
JOIN fact_orders o
    ON d.order_id = o.order_id
JOIN dim_merchant m
    ON o.merchant_sk = m.merchant_sk
    AND m.is_current = true
GROUP BY m.merchant_name
ORDER BY avg_delay_days DESC;


-- -----------------------------------------------------
-- 4.4 Delay analysis by order size, product type, and campaign usage
-- -----------------------------------------------------
CREATE OR REPLACE VIEW delay_vs_order_characteristics AS
WITH order_items AS (
    SELECT
        li.order_id,
        SUM(li.line_item_quantity) AS total_items,
        STRING_AGG(DISTINCT p.product_type, ', ') AS product_types
    FROM fact_line_items li
    JOIN dim_product p
        ON li.product_id = p.product_id
    GROUP BY li.order_id
),
campaign_orders AS (
    SELECT DISTINCT order_id, TRUE AS availed_campaign
    FROM fact_campaign_transactions
)
SELECT
    o.order_id,
    oi.total_items,
    oi.product_types,
    COALESCE(co.availed_campaign, FALSE) AS availed_campaign,
    d.delay_in_days
FROM fact_orders o
LEFT JOIN order_items oi ON o.order_id = oi.order_id
LEFT JOIN campaign_orders co ON o.order_id = co.order_id
JOIN fact_order_delays d ON o.order_id = d.order_id;


-- -----------------------------------------------------
-- 4.5 Seasonal delivery delay patterns
-- -----------------------------------------------------
CREATE OR REPLACE VIEW seasonal_delivery_delays AS
SELECT
    dt.date_year,
    dt.date_month,
    SUM(CASE WHEN d.delay_in_days > 0 THEN 1 ELSE 0 END) AS delayed_orders,
    AVG(d.delay_in_days) AS avg_delay_days
FROM fact_order_delays d
JOIN fact_orders o
    ON d.order_id = o.order_id
JOIN dim_date dt
    ON o.order_transaction_date = dt.date_value
GROUP BY dt.date_year, dt.date_month
ORDER BY dt.date_year, dt.date_month;
