-- ================================
-- FACT TABLES (Improved)
-- ================================

------------------------------------
-- 1. FACT ORDERS
------------------------------------
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id              VARCHAR PRIMARY KEY,
    user_id               VARCHAR,
    order_transaction_date DATE,
    order_estimated_arrival INT,
    merchant_id           VARCHAR,
    staff_id              VARCHAR,

    CONSTRAINT fk_orders_user
        FOREIGN KEY (user_id) REFERENCES dim_user(user_id),

    CONSTRAINT fk_orders_merchant
        FOREIGN KEY (merchant_id) REFERENCES dim_merchant(merchant_id),

    CONSTRAINT fk_orders_staff
        FOREIGN KEY (staff_id) REFERENCES dim_staff(staff_id),

    CONSTRAINT fk_orders_date
        FOREIGN KEY (order_transaction_date) REFERENCES dim_date(date_value)
);

------------------------------------
-- 2. FACT LINE ITEMS
------------------------------------
CREATE TABLE IF NOT EXISTS fact_line_items (
    order_id         VARCHAR,
    product_id       VARCHAR,
    line_item_price  DECIMAL(10,2),
    line_item_quantity INT,
    line_item_product_name VARCHAR,

    PRIMARY KEY (order_id, product_id),

    CONSTRAINT fk_lineitems_order
        FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),

    CONSTRAINT fk_lineitems_product
        FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
);

------------------------------------
-- 3. FACT CAMPAIGN TRANSACTIONS
------------------------------------
CREATE TABLE IF NOT EXISTS fact_campaign_transactions (
    transaction_date        DATE,
    campaign_id             VARCHAR,
    order_id                VARCHAR,
    txn_estimated_arrival   INT,
    transaction_availed     BOOLEAN,

    PRIMARY KEY (transaction_date, campaign_id, order_id),

    CONSTRAINT fk_campaign_txn_campaign
        FOREIGN KEY (campaign_id) REFERENCES dim_campaign(campaign_id),

    CONSTRAINT fk_campaign_txn_order
        FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),

    CONSTRAINT fk_campaign_txn_date
        FOREIGN KEY (transaction_date) REFERENCES dim_date(date_value)
);

------------------------------------
-- 4. FACT ORDER DELAYS
------------------------------------
CREATE TABLE IF NOT EXISTS fact_order_delays (
    order_id            VARCHAR PRIMARY KEY,
    delay_in_days       INT,

    CONSTRAINT fk_delays_order
        FOREIGN KEY (order_id) REFERENCES fact_orders(order_id)
);
