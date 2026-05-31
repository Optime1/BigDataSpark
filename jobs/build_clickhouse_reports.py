import os

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F


PG_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/bigdataspark")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
CH_URL = os.getenv("CLICKHOUSE_URL", "jdbc:clickhouse://clickhouse:8123/default")
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")


def pg_props():
    return {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver",
    }


def write_clickhouse(df, table_name):
    (
        df.write.format("jdbc")
        .mode("overwrite")
        .option("url", CH_URL)
        .option("dbtable", table_name)
        .option("user", CH_USER)
        .option("password", CH_PASSWORD)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("createTableOptions", "ENGINE = MergeTree() ORDER BY tuple()")
        .save()
    )


spark = (
    SparkSession.builder.appName("BigDataSpark ClickHouse reports")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

fact = spark.read.jdbc(PG_URL, "fact_sales", properties=pg_props())
products = spark.read.jdbc(PG_URL, "dim_product", properties=pg_props()).select(
    "product_key",
    "product_name",
    F.col("category").alias("product_category"),
    "product_rating_num",
    "product_reviews_int",
)
customers = spark.read.jdbc(PG_URL, "dim_customer", properties=pg_props()).select(
    "customer_key",
    F.col("first_name").alias("customer_first_name"),
    F.col("last_name").alias("customer_last_name"),
    F.col("email").alias("customer_email"),
    F.col("country").alias("customer_country"),
)
stores = spark.read.jdbc(PG_URL, "dim_store", properties=pg_props()).select(
    "store_key",
    "store_name",
    F.col("city").alias("store_city"),
    F.col("country").alias("store_country"),
)
suppliers = spark.read.jdbc(PG_URL, "dim_supplier", properties=pg_props()).select(
    "supplier_key",
    "supplier_name",
    F.col("country").alias("supplier_country"),
)
dates = spark.read.jdbc(PG_URL, "dim_date", properties=pg_props())

sales = (
    fact.join(products, "product_key", "left")
    .join(customers, "customer_key", "left")
    .join(stores, "store_key", "left")
    .join(suppliers, "supplier_key", "left")
    .join(dates, "date_key", "left")
)

product_sales = (
    sales.groupBy("product_key", "product_name", "product_category")
    .agg(
        F.sum("sale_total_price_num").alias("total_revenue"),
        F.sum("sale_quantity_int").alias("total_quantity_sold"),
        F.count("*").alias("sales_count"),
        F.avg("product_rating_num").alias("avg_rating"),
        F.max("product_reviews_int").alias("reviews_count"),
    )
    .withColumn("category_revenue", F.sum("total_revenue").over(Window.partitionBy("product_category")))
    .withColumn("sales_rank", F.dense_rank().over(Window.orderBy(F.desc("total_quantity_sold"), F.desc("total_revenue"))))
)

customer_country_counts = customers.groupBy("customer_country").agg(F.countDistinct("customer_key").alias("country_customer_count"))
customer_sales = (
    sales.groupBy("customer_key", "customer_first_name", "customer_last_name", "customer_email", "customer_country")
    .agg(
        F.sum("sale_total_price_num").alias("total_purchase_amount"),
        F.count("*").alias("orders_count"),
        F.avg("sale_total_price_num").alias("avg_order_value"),
    )
    .join(customer_country_counts, "customer_country", "left")
    .withColumn("purchase_rank", F.dense_rank().over(Window.orderBy(F.desc("total_purchase_amount"))))
)

time_sales = (
    sales.groupBy("year", "month")
    .agg(
        F.sum("sale_total_price_num").alias("monthly_revenue"),
        F.count("*").alias("orders_count"),
        F.avg("sale_total_price_num").alias("avg_order_size"),
    )
    .withColumn("yearly_revenue", F.sum("monthly_revenue").over(Window.partitionBy("year")))
    .withColumn("period", F.concat_ws("-", F.col("year").cast("string"), F.lpad(F.col("month").cast("string"), 2, "0")))
    .select("period", "year", "month", "monthly_revenue", "yearly_revenue", "orders_count", "avg_order_size")
)

store_sales = (
    sales.groupBy("store_key", "store_name", "store_city", "store_country")
    .agg(
        F.sum("sale_total_price_num").alias("total_revenue"),
        F.count("*").alias("orders_count"),
        F.avg("sale_total_price_num").alias("avg_check"),
    )
    .withColumn("city_revenue", F.sum("total_revenue").over(Window.partitionBy("store_city")))
    .withColumn("country_revenue", F.sum("total_revenue").over(Window.partitionBy("store_country")))
    .withColumn("store_rank", F.dense_rank().over(Window.orderBy(F.desc("total_revenue"))))
)

supplier_sales = (
    sales.groupBy("supplier_key", "supplier_name", "supplier_country")
    .agg(
        F.sum("sale_total_price_num").alias("total_revenue"),
        F.avg("product_price_num").alias("avg_product_price"),
        F.sum("sale_quantity_int").alias("total_quantity_sold"),
    )
    .withColumn("supplier_country_revenue", F.sum("total_revenue").over(Window.partitionBy("supplier_country")))
    .withColumn("supplier_rank", F.dense_rank().over(Window.orderBy(F.desc("total_revenue"))))
)

rating_corr = sales.agg(F.corr("product_rating_num", "sale_quantity_int").alias("rating_sales_correlation"))
product_quality = (
    sales.groupBy("product_key", "product_name", "product_category")
    .agg(
        F.avg("product_rating_num").alias("avg_rating"),
        F.max("product_reviews_int").alias("reviews_count"),
        F.sum("sale_quantity_int").alias("sales_volume"),
    )
    .crossJoin(rating_corr)
    .withColumn("rating_rank_high", F.dense_rank().over(Window.orderBy(F.desc("avg_rating"))))
    .withColumn("rating_rank_low", F.dense_rank().over(Window.orderBy(F.asc("avg_rating"))))
    .withColumn("reviews_rank", F.dense_rank().over(Window.orderBy(F.desc("reviews_count"))))
)

write_clickhouse(product_sales, "mart_product_sales")
write_clickhouse(customer_sales, "mart_customer_sales")
write_clickhouse(time_sales, "mart_time_sales")
write_clickhouse(store_sales, "mart_store_sales")
write_clickhouse(supplier_sales, "mart_supplier_sales")
write_clickhouse(product_quality, "mart_product_quality")

spark.stop()
