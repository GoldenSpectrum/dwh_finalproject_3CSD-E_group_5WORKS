-- =====================================================
-- ORDER-LEVEL ANALYTICS
-- Uses: fact_orders + dim_date + dim_user + dim_staff + dim_merchant
-- =====================================================

-- -----------------------------------------------------
-- 1.1 Orders per day
-- -----------------------------------------------------
CREATE OR REPLACE VIEW orders_per_day AS
SELECT
    d.date_value,
    COUNT(*) AS orders_count
FROM fact_orders o
JOIN dim_date d
    ON o.order_transaction_date = d.date_value
GROUP BY d.date_value
ORDER BY d.date_value;


-- -----------------------------------------------------
-- 1.1 Orders per week
-- -----------------------------------------------------
CREATE OR REPLACE VIEW orders_per_week AS
SELECT
    d.date_year,
    d.week_of_year,
    COUNT(*) AS orders_count
FROM fact_orders o
JOIN dim_date d
    ON o.order_transaction_date = d.date_value
GROUP BY d.date_year, d.week_of_year
ORDER BY d.date_year, d.week_of_year;


-- -----------------------------------------------------
-- 1.1 Orders per month
-- -----------------------------------------------------
CREATE OR REPLACE VIEW orders_per_month AS
SELECT
    d.date_year,
    d.date_month,
    COUNT(*) AS orders_count
FROM fact_orders o
JOIN dim_date d
    ON o.order_transaction_date = d.date_value
GROUP BY d.date_year, d.date_month
ORDER BY d.date_year, d.date_month;


-- -----------------------------------------------------
-- 1.2 Average estimated arrival time per merchant
-- -----------------------------------------------------
CREATE OR REPLACE VIEW avg_estimated_arrival_per_merchant AS
SELECT
    m.merchant_name,
    AVG(o.order_estimated_arrival) AS avg_estimated_days
FROM fact_orders o
JOIN dim_merchant m
    ON o.merchant_sk = m.merchant_sk
    AND m.is_current = true
GROUP BY m.merchant_name
ORDER BY avg_estimated_days;


-- -----------------------------------------------------
-- 1.3 Users with highest number of orders
-- -----------------------------------------------------
CREATE OR REPLACE VIEW top_users_by_orders AS
SELECT
    u.user_id,
    u.user_full_name,
    COUNT(o.order_id) AS total_orders
FROM fact_orders o
LEFT JOIN dim_user u
    ON o.user_sk = u.user_sk
    AND u.is_current = true
GROUP BY u.user_id, u.user_full_name
ORDER BY total_orders DESC;


-- -----------------------------------------------------
-- 1.4 Total orders fulfilled by each staff member
-- -----------------------------------------------------
CREATE OR REPLACE VIEW staff_order_fulfillment AS
SELECT
    s.staff_full_name,
    COUNT(o.order_id) AS orders_fulfilled
FROM fact_orders o
JOIN dim_staff s
    ON o.staff_sk = s.staff_sk
    AND s.is_current = true
GROUP BY s.staff_full_name
ORDER BY orders_fulfilled DESC;


-- -----------------------------------------------------
-- 1.5 Monthly order volume (growth trend baseline)
-- -----------------------------------------------------
CREATE OR REPLACE VIEW monthly_order_volume AS
SELECT
    d.date_year,
    d.date_month,
    COUNT(o.order_id) AS total_orders
FROM fact_orders o
JOIN dim_date d
    ON o.order_transaction_date = d.date_value
GROUP BY d.date_year, d.date_month
ORDER BY d.date_year, d.date_month;
