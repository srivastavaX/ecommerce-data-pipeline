CREATE SCHEMA IF NOT EXISTS raw;


CREATE TABLE IF NOT EXISTS raw.products (
    id INT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    category TEXT,
    price NUMERIC(10, 2),
    discount_percentage NUMERIC(5, 2),
    rating NUMERIC(3, 2),
    stock INT,
    brand TEXT,
    sku TEXT UNIQUE,
    availability_status TEXT,
    minimum_order_quantity INT
);

CREATE TABLE IF NOT EXISTS raw.users (
    id INT PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    maiden_name TEXT,
    age INT,
    gender TEXT,
    email TEXT UNIQUE NOT NULL,
    phone TEXT,
    image TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
);

CREATE TABLE IF NOT EXISTS raw.carts (
    id INT PRIMARY KEY,
    user_id INT NOT NULL REFERENCES raw.users(id),
    total NUMERIC(10, 2),
    discounted_total NUMERIC(10, 2),
    total_products INT,
    total_quantity INT
);

CREATE TABLE IF NOT EXISTS raw.cart_items (
    cart_id INT NOT NULL REFERENCES raw.carts(id),
    product_id INT NOT NULL REFERENCES raw.products(id),
    title TEXT,
    price NUMERIC(10, 2),
    quantity INT,
    total NUMERIC(10, 2),
    discounted_price NUMERIC(10, 2),
    discount_percentage NUMERIC(5, 2),

    PRIMARY KEY (cart_id, product_id)  -- composite pk
);