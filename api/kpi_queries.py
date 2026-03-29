OPEN_ORDERS_BY_DATE_STATUS = """
    SELECT
        delivery_date,
        status,
        COUNT(DISTINCT order_id) AS order_count
    FROM analytics.open_orders
    GROUP BY delivery_date, status
    ORDER BY delivery_date, status
"""

TOP3_DELIVERY_DATES = """
    SELECT
        delivery_date,
        COUNT(DISTINCT order_id) AS open_order_count
    FROM analytics.open_orders
    GROUP BY delivery_date
    ORDER BY open_order_count DESC
    LIMIT 3
"""

PENDING_ITEMS_BY_PRODUCT = """
    SELECT
        product_id,
        COUNT(*) AS pending_item_count
    FROM analytics.open_orders
    WHERE status = 'PENDING'
    GROUP BY product_id
    ORDER BY pending_item_count DESC
"""

TOP3_CUSTOMERS_PENDING = """
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS pending_order_count
    FROM analytics.open_orders
    WHERE status = 'PENDING'
    GROUP BY customer_id
    ORDER BY pending_order_count DESC
    LIMIT 3
"""