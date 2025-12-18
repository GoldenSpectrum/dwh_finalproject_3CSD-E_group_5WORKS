-- =====================================================
-- CAMPAIGN ANALYTICS
-- Uses: fact_campaign_transactions + fact_orders + fact_line_items
--        + dim_campaign + dim_user
-- =====================================================

-- -----------------------------------------------------
-- 3.1 Campaigns with highest number of redemptions
-- -----------------------------------------------------
CREATE OR REPLACE VIEW campaign_redemptions AS
SELECT
    c.campaign_name,
    COUNT(*) FILTER (WHERE f.transaction_availed = TRUE) AS total_redemptions
FROM fact_campaign_transactions f
JOIN dim_campaign c
    ON f.campaign_id = c.campaign_id
GROUP BY c.campaign_name
ORDER BY total_redemptions DESC;


-- -----------------------------------------------------
-- 3.2 Campaign impact on total order value per user
-- -----------------------------------------------------
CREATE OR REPLACE VIEW campaign_order_value_by_user AS
SELECT
    u.user_full_name,
    SUM(li.line_item_price * li.line_item_quantity) AS total_order_value,
    COUNT(DISTINCT f.order_id) AS campaign_orders
FROM fact_campaign_transactions f
JOIN fact_orders o
    ON f.order_id = o.order_id
JOIN dim_user u
    ON o.user_sk = u.user_sk
    AND u.is_current = true
JOIN fact_line_items li
    ON li.order_id = o.order_id
GROUP BY u.user_full_name
ORDER BY total_order_value DESC;


-- -----------------------------------------------------
-- 3.3 User segments redeeming campaigns most frequently
-- -----------------------------------------------------
CREATE OR REPLACE VIEW campaign_redemptions_by_user_segment AS
SELECT
    u.user_type,
    COUNT(*) AS total_redemptions
FROM fact_campaign_transactions f
JOIN fact_orders o
    ON f.order_id = o.order_id
JOIN dim_user u
    ON o.user_sk = u.user_sk
    AND u.is_current = true
WHERE f.transaction_availed = TRUE
GROUP BY u.user_type
ORDER BY total_redemptions DESC;


-- -----------------------------------------------------
-- 3.4 Campaign conversion rate (availed vs offered)
-- -----------------------------------------------------
CREATE OR REPLACE VIEW campaign_conversion_rate AS
SELECT
    c.campaign_name,
    COUNT(*) AS total_offered,
    COUNT(*) FILTER (WHERE f.transaction_availed = TRUE) AS total_availed,
    ROUND(
        COUNT(*) FILTER (WHERE f.transaction_availed = TRUE)::numeric
        / NULLIF(COUNT(*), 0) * 100,
        2
    ) AS conversion_rate_pct
FROM fact_campaign_transactions f
JOIN dim_campaign c
    ON f.campaign_id = c.campaign_id
GROUP BY c.campaign_name
ORDER BY conversion_rate_pct DESC;


-- -----------------------------------------------------
-- 3.5 Campaigns driving repeat purchases
-- -----------------------------------------------------
CREATE OR REPLACE VIEW campaign_repeat_purchases AS
WITH repeat_users AS (
    SELECT user_sk
    FROM fact_orders
    GROUP BY user_sk
    HAVING COUNT(order_id) > 1
)
SELECT
    c.campaign_name,
    COUNT(DISTINCT o.user_sk) AS unique_users,
    COUNT(DISTINCT CASE
        WHEN o.user_sk IN (SELECT user_sk FROM repeat_users)
        THEN o.order_id
    END) AS repeat_purchase_count
FROM fact_campaign_transactions f
JOIN fact_orders o
    ON f.order_id = o.order_id
JOIN dim_campaign c
    ON f.campaign_id = c.campaign_id
WHERE f.transaction_availed = TRUE
GROUP BY c.campaign_name
ORDER BY repeat_purchase_count DESC;
