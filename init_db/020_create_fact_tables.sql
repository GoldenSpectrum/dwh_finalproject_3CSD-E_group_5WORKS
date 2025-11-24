-- ================================
-- FACT TABLES (Kimball Style)
-- ================================

------------------------------------
-- 1. FACT ORDERS
-- Grain: One row per order
------------------------------------
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id           VARCHAR PRIMARY KEY,
    user_id            VARCHAR,
    transaction_date   DATE,
    estimated_arrival  INT,
    merchant_id        VARCHAR,
    staff_id           VARCHAR,
    delay_in_days      INT,

    CONSTRAINT fk_orders_user
        FOREIGN KEY (user_id) REFERENCES dim_user(user_id),

    CONSTRAINT fk_orders_merchant
        FOREIGN KEY (merchant_id) REFERENCES dim_merchant(merchant_id),

    CONSTRAINT fk_orders_staff
        FOREIGN KEY (staff_id) REFERENCES dim_staff(staff_id),

    CONSTRAINT fk_orders_date
        FOREIGN KEY (transaction_date) REFERENCES dim_date(date)
);

------------------------------------
-- 2. FACT LINE ITEMS
-- Grain: One row per order/product combination
------------------------------------
CREATE TABLE IF NOT EXISTS fact_line_items (
    order_id       VARCHAR,
    product_id     VARCHAR,
    price          DECIMAL(10,2),
    quantity       INT,
    product_name   VARCHAR,

    -- Composite Primary Key
    PRIMARY KEY (order_id, product_id),

    CONSTRAINT fk_lineitems_order
        FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),

    CONSTRAINT fk_lineitems_product
        FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
);

------------------------------------
-- 3. FACT CAMPAIGN TRANSACTIONS
-- Grain: One row per campaign per order per date
------------------------------------
CREATE TABLE IF NOT EXISTS fact_campaign_transactions (
    transaction_date   DATE,
    campaign_id        VARCHAR,
    order_id           VARCHAR,
    estimated_arrival  INT,
    availed            BOOLEAN,

    -- Composite Primary Key
    PRIMARY KEY (transaction_date, campaign_id, order_id),

    CONSTRAINT fk_campaign_txn_campaign
        FOREIGN KEY (campaign_id) REFERENCES dim_campaign(campaign_id),

    CONSTRAINT fk_campaign_txn_order
        FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),

    CONSTRAINT fk_campaign_txn_date
        FOREIGN KEY (transaction_date) REFERENCES dim_date(date)
);

------------------------------------
-- 4. FACT ORDER DELAYS
-- Grain: One row per order's delay record
------------------------------------
CREATE TABLE IF NOT EXISTS fact_order_delays (
    order_id        VARCHAR PRIMARY KEY,
    delay_in_days   INT,

    CONSTRAINT fk_delays_order
        FOREIGN KEY (order_id) REFERENCES fact_orders(order_id)
);
