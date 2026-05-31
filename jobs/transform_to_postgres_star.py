import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PG_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/bigdataspark")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


def jdbc_props():
    return {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver",
    }


def clean_string(column_name):
    return F.trim(F.col(column_name))


def as_int(column_name):
    return F.col(column_name).cast("int")


def as_double(column_name):
    return F.regexp_replace(F.col(column_name), ",", ".").cast("double")


def parse_date(column_name):
    return F.coalesce(
        F.to_date(F.col(column_name), "M/d/yyyy"),
        F.to_date(F.col(column_name), "MM/dd/yyyy"),
    )


def dim_id(*columns):
    values = [F.coalesce(F.col(column).cast("string"), F.lit("")) for column in columns]
    return F.sha2(F.concat_ws("||", *values), 256)


def write_pg(df, table_name):
    df.write.jdbc(PG_URL, table_name, mode="overwrite", properties=jdbc_props())


spark = (
    SparkSession.builder.appName("BigDataSpark PostgreSQL star schema")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

raw = spark.read.jdbc(PG_URL, "raw_mock_data", properties=jdbc_props())

base = (
    raw.withColumn("customer_age_int", as_int("customer_age"))
    .withColumn("product_price_num", as_double("product_price"))
    .withColumn("product_quantity_int", as_int("product_quantity"))
    .withColumn("sale_quantity_int", as_int("sale_quantity"))
    .withColumn("sale_total_price_num", as_double("sale_total_price"))
    .withColumn("product_weight_num", as_double("product_weight"))
    .withColumn("product_rating_num", as_double("product_rating"))
    .withColumn("product_reviews_int", as_int("product_reviews"))
    .withColumn("sale_dt", parse_date("sale_date"))
    .withColumn("product_release_dt", parse_date("product_release_date"))
    .withColumn("product_expiry_dt", parse_date("product_expiry_date"))
)

customer_key_cols = [
    "sale_customer_id",
    "customer_first_name",
    "customer_last_name",
    "customer_email",
    "customer_country",
    "customer_postal_code",
]
seller_key_cols = [
    "sale_seller_id",
    "seller_first_name",
    "seller_last_name",
    "seller_email",
    "seller_country",
    "seller_postal_code",
]
product_key_cols = [
    "sale_product_id",
    "product_name",
    "product_category",
    "product_brand",
    "product_material",
    "product_size",
    "product_color",
    "supplier_name",
]
store_key_cols = ["store_name", "store_location", "store_city", "store_state", "store_country"]
supplier_key_cols = ["supplier_name", "supplier_email", "supplier_phone", "supplier_city", "supplier_country"]

enriched = (
    base.withColumn("customer_key", dim_id(*customer_key_cols))
    .withColumn("seller_key", dim_id(*seller_key_cols))
    .withColumn("product_key", dim_id(*product_key_cols))
    .withColumn("store_key", dim_id(*store_key_cols))
    .withColumn("supplier_key", dim_id(*supplier_key_cols))
    .withColumn("date_key", F.date_format(F.col("sale_dt"), "yyyyMMdd").cast("int"))
    .withColumn("sale_key", dim_id("raw_file", "id", "sale_date", "sale_customer_id", "sale_product_id", "sale_total_price"))
)

dim_customer = enriched.select(
    "customer_key",
    clean_string("sale_customer_id").alias("source_customer_id"),
    clean_string("customer_first_name").alias("first_name"),
    clean_string("customer_last_name").alias("last_name"),
    "customer_age_int",
    clean_string("customer_email").alias("email"),
    clean_string("customer_country").alias("country"),
    clean_string("customer_postal_code").alias("postal_code"),
    clean_string("customer_pet_type").alias("pet_type"),
    clean_string("customer_pet_name").alias("pet_name"),
    clean_string("customer_pet_breed").alias("pet_breed"),
).dropDuplicates(["customer_key"])

dim_seller = enriched.select(
    "seller_key",
    clean_string("sale_seller_id").alias("source_seller_id"),
    clean_string("seller_first_name").alias("first_name"),
    clean_string("seller_last_name").alias("last_name"),
    clean_string("seller_email").alias("email"),
    clean_string("seller_country").alias("country"),
    clean_string("seller_postal_code").alias("postal_code"),
).dropDuplicates(["seller_key"])

dim_supplier = enriched.select(
    "supplier_key",
    clean_string("supplier_name").alias("supplier_name"),
    clean_string("supplier_contact").alias("contact"),
    clean_string("supplier_email").alias("email"),
    clean_string("supplier_phone").alias("phone"),
    clean_string("supplier_address").alias("address"),
    clean_string("supplier_city").alias("city"),
    clean_string("supplier_country").alias("country"),
).dropDuplicates(["supplier_key"])

dim_product = enriched.select(
    "product_key",
    "supplier_key",
    clean_string("sale_product_id").alias("source_product_id"),
    clean_string("product_name").alias("product_name"),
    clean_string("product_category").alias("category"),
    "product_price_num",
    "product_quantity_int",
    clean_string("pet_category").alias("pet_category"),
    "product_weight_num",
    clean_string("product_color").alias("color"),
    clean_string("product_size").alias("size"),
    clean_string("product_brand").alias("brand"),
    clean_string("product_material").alias("material"),
    clean_string("product_description").alias("description"),
    "product_rating_num",
    "product_reviews_int",
    "product_release_dt",
    "product_expiry_dt",
).dropDuplicates(["product_key"])

dim_store = enriched.select(
    "store_key",
    clean_string("store_name").alias("store_name"),
    clean_string("store_location").alias("location"),
    clean_string("store_city").alias("city"),
    clean_string("store_state").alias("state"),
    clean_string("store_country").alias("country"),
    clean_string("store_phone").alias("phone"),
    clean_string("store_email").alias("email"),
).dropDuplicates(["store_key"])

dim_date = enriched.where(F.col("sale_dt").isNotNull()).select(
    "date_key",
    F.col("sale_dt").alias("date"),
    F.year("sale_dt").alias("year"),
    F.quarter("sale_dt").alias("quarter"),
    F.month("sale_dt").alias("month"),
    F.dayofmonth("sale_dt").alias("day"),
    F.date_format("sale_dt", "MMMM").alias("month_name"),
).dropDuplicates(["date_key"])

fact_sales = enriched.select(
    "sale_key",
    "customer_key",
    "seller_key",
    "product_key",
    "store_key",
    "supplier_key",
    "date_key",
    F.col("raw_file").alias("source_file"),
    F.col("id").alias("source_row_id"),
    "sale_dt",
    "sale_quantity_int",
    "sale_total_price_num",
    "product_price_num",
)

write_pg(dim_customer, "dim_customer")
write_pg(dim_seller, "dim_seller")
write_pg(dim_supplier, "dim_supplier")
write_pg(dim_product, "dim_product")
write_pg(dim_store, "dim_store")
write_pg(dim_date, "dim_date")
write_pg(fact_sales, "fact_sales")

spark.stop()
