CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.open_orders (
    order_id        BIGINT,
    delivery_date   DATE,
    status          VARCHAR(50),
    customer_id     BIGINT,
    order_date      DATE,
    order_item_id   BIGINT,
    product_id      BIGINT,
    quantity        INTEGER
);


CREATE INDEX IF NOT EXISTS idx_open_orders_delivery_status
    ON analytics.open_orders(delivery_date, status);

CREATE INDEX IF NOT EXISTS idx_open_orders_status_product
    ON analytics.open_orders(status, product_id);

CREATE INDEX IF NOT EXISTS idx_open_orders_status_customer
    ON analytics.open_orders(status, customer_id);