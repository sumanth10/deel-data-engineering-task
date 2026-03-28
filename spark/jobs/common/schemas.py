from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, StringType, BooleanType
)

# source block (common to all tables)
SOURCE_SCHEMA = StructType([
    StructField("version",   StringType(), True),
    StructField("connector", StringType(), True),
    StructField("name",      StringType(), True),
    StructField("ts_ms",     LongType(),   True),
    StructField("snapshot",  StringType(), True),
    StructField("db",        StringType(), True),
    StructField("schema",    StringType(), True),
    StructField("table",     StringType(), True),
    StructField("txId",      LongType(),   True),
    StructField("lsn",       LongType(),   True),
    StructField("xmin",      LongType(),   True),
])

# row schemas (the shape of "before" and "after")

ORDERS_ROW = StructType([
    StructField("order_id",       LongType(),    False),
    StructField("order_date",     IntegerType(), True),
    StructField("delivery_date",  IntegerType(), True),
    StructField("customer_id",    LongType(),    True),
    StructField("status",         StringType(),  True),
    StructField("updated_at",     LongType(),    True),
    StructField("updated_by",     LongType(),    True),
    StructField("created_at",     LongType(),    True),
    StructField("created_by",     LongType(),    True),
])

CUSTOMERS_ROW = StructType([
    StructField("customer_id",      LongType(),    False),
    StructField("customer_name",    StringType(),  True),
    StructField("is_active",        BooleanType(), True),
    StructField("customer_address", StringType(),  True),
    StructField("updated_at",       LongType(),    True),
    StructField("updated_by",       LongType(),    True),
    StructField("created_at",       LongType(),    True),
    StructField("created_by",       LongType(),    True),
])

PRODUCTS_ROW = StructType([
    StructField("product_id",   LongType(),    False),
    StructField("product_name", StringType(),  True),
    StructField("barcode",      StringType(),  True),
    StructField("unity_price",  StringType(),  True),
    StructField("is_active",    BooleanType(), True),
    StructField("updated_at",   LongType(),    True),
    StructField("updated_by",   LongType(),    True),
    StructField("created_at",   LongType(),    True),
    StructField("created_by",   LongType(),    True),
])

ORDER_ITEMS_ROW = StructType([
    StructField("order_item_id", LongType(),    False),
    StructField("order_id",      LongType(),    True),
    StructField("product_id",    LongType(),    True),
    StructField("quanity",      IntegerType(), True),
    StructField("updated_at",    LongType(),    True),
    StructField("updated_by",    LongType(),    True),
    StructField("created_at",    LongType(),    True),
    StructField("created_by",    LongType(),    True),
])

def make_envelope(row_schema: StructType) -> StructType:
    return StructType([
        StructField("before",  row_schema,    True),
        StructField("after",   row_schema,    True),
        StructField("op",      StringType(),  False),
        StructField("ts_ms",   LongType(),    False),
        StructField("source",  SOURCE_SCHEMA, True),
    ])

ORDERS_SCHEMA      = make_envelope(ORDERS_ROW)
CUSTOMERS_SCHEMA   = make_envelope(CUSTOMERS_ROW)
PRODUCTS_SCHEMA    = make_envelope(PRODUCTS_ROW)
ORDER_ITEMS_SCHEMA = make_envelope(ORDER_ITEMS_ROW)

TOPIC_SCHEMA_MAP = {
    "finance_db.operations.orders":      ORDERS_SCHEMA,
    "finance_db.operations.customers":   CUSTOMERS_SCHEMA,
    "finance_db.operations.products":    PRODUCTS_SCHEMA,
    "finance_db.operations.order_items": ORDER_ITEMS_SCHEMA,
}