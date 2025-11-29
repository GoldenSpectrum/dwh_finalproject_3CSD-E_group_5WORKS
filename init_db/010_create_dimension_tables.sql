
CREATE TABLE IF NOT EXISTS dim_user (
    user_id                 VARCHAR PRIMARY KEY,
    user_creation_date      DATE,
    user_full_name          VARCHAR,
    user_street_address     VARCHAR,
    user_state              VARCHAR,
    user_city               VARCHAR,
    user_country            VARCHAR,
    user_birthdate          DATE,
    user_gender             VARCHAR,
    user_device_address     VARCHAR,
    user_type               VARCHAR,
    user_job_title          VARCHAR,
    user_job_level          VARCHAR,
    user_credit_card_number BIGINT,
    user_issuing_bank       VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_id            VARCHAR PRIMARY KEY,
    merchant_creation_date DATE,
    merchant_name          VARCHAR,
    merchant_street_address VARCHAR,
    merchant_state         VARCHAR,
    merchant_city          VARCHAR,
    merchant_country       VARCHAR,
    merchant_contact_number VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_staff (
    staff_id              VARCHAR PRIMARY KEY,
    staff_full_name       VARCHAR,
    staff_job_level       VARCHAR,
    staff_street_address  VARCHAR,
    staff_state           VARCHAR,
    staff_city            VARCHAR,
    staff_country         VARCHAR,
    staff_contact_number  VARCHAR,
    staff_creation_date   DATE
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id          VARCHAR PRIMARY KEY,
    product_name        VARCHAR,
    product_type        VARCHAR,
    product_price       DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_id            VARCHAR PRIMARY KEY,
    campaign_name          VARCHAR,
    campaign_description   TEXT,
    campaign_discount      DECIMAL(5,2)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id          SERIAL PRIMARY KEY,
    date_value       DATE UNIQUE NOT NULL,
    date_year        INT,
    date_quarter     INT,
    date_month       INT,
    date_day         INT,
    day_of_week      INT,
    is_weekend       BOOLEAN,
    week_start_date  DATE,
    week_of_year     INT
);

