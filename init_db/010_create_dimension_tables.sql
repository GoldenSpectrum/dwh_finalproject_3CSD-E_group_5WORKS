-- ================================
-- DIMENSION TABLES
-- ================================

CREATE TABLE IF NOT EXISTS dim_user (
    user_id            VARCHAR PRIMARY KEY,
    creation_date      DATE,
    name               VARCHAR,
    street             VARCHAR,
    state              VARCHAR,
    city               VARCHAR,
    country            VARCHAR,
    birthdate          DATE,
    gender             VARCHAR,
    device_address     VARCHAR,
    user_type          VARCHAR,
    job_title          VARCHAR,
    job_level          VARCHAR,
    credit_card_number BIGINT,
    issuing_bank       VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_id     VARCHAR PRIMARY KEY,
    creation_date   DATE,
    name            VARCHAR,
    street          VARCHAR,
    state           VARCHAR,
    city            VARCHAR,
    country         VARCHAR,
    contact_number  VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_staff (
    staff_id        VARCHAR PRIMARY KEY,
    name            VARCHAR,
    job_level       VARCHAR,
    street          VARCHAR,
    state           VARCHAR,
    city            VARCHAR,
    country         VARCHAR,
    contact_number  VARCHAR,
    creation_date   DATE
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id      VARCHAR PRIMARY KEY,
    product_name    VARCHAR,
    product_type    VARCHAR,
    price           DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_id            VARCHAR PRIMARY KEY,
    campaign_name          VARCHAR,
    campaign_description   TEXT,
    discount               DECIMAL(5,2)
);

-- Date dimension: date is the PK
CREATE TABLE IF NOT EXISTS dim_date (
    date DATE PRIMARY KEY,
    year INT,
    quarter INT,
    month INT,
    day INT,
    day_of_week INT,
    is_weekend BOOLEAN
);

