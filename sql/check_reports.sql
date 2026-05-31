-- PostgreSQL checks
SELECT count(*) AS raw_rows FROM raw_mock_data;
SELECT count(*) AS fact_rows FROM fact_sales;
SELECT count(*) AS customers FROM dim_customer;
SELECT count(*) AS products FROM dim_product;
SELECT count(*) AS stores FROM dim_store;

-- ClickHouse checks
SELECT count(*) AS product_sales_rows FROM mart_product_sales;
SELECT count(*) AS customer_sales_rows FROM mart_customer_sales;
SELECT count(*) AS time_sales_rows FROM mart_time_sales;
SELECT count(*) AS store_sales_rows FROM mart_store_sales;
SELECT count(*) AS supplier_sales_rows FROM mart_supplier_sales;
SELECT count(*) AS product_quality_rows FROM mart_product_quality;
