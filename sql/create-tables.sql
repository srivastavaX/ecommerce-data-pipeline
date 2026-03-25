CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id     INTEGER PRIMARY KEY,
    first_name      TEXT        NOT NULL,
    last_name       TEXT        NOT NULL,
    email           TEXT        UNIQUE NOT NULL,
    phone           TEXT,
    country         TEXT,
    city            TEXT,
    created_at      TIMESTAMP   DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id      INTEGER PRIMARY KEY,
    name            TEXT            NOT NULL,
    category        TEXT,
    price           NUMERIC(10, 2)  NOT NULL,
    stock           INTEGER         DEFAULT 0,
    rating          NUMERIC(3, 2)
);

-- -------------------------------------------------------------
-- ORDERS
-- Source: DummyJSON /carts endpoint (one cart = one order)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id        INTEGER PRIMARY KEY,
    customer_id     INTEGER         REFERENCES raw.customers(customer_id),
    order_date      TIMESTAMP       DEFAULT NOW(),
    status          TEXT            DEFAULT 'completed',
    total_amount    NUMERIC(10, 2)
);

-- -------------------------------------------------------------
-- ORDER ITEMS
-- Source: Nested products array inside each DummyJSON cart
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id   SERIAL          PRIMARY KEY,
    order_id        INTEGER         REFERENCES raw.orders(order_id),
    product_id      INTEGER         REFERENCES raw.products(product_id),
    quantity        INTEGER         NOT NULL DEFAULT 1,
    price           NUMERIC(10, 2)  NOT NULL
);