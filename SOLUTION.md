# BigDataSpark solution

Обязательная часть лабораторной реализована для PostgreSQL, Spark и ClickHouse.

## Состав

- `docker-compose.yml` поднимает PostgreSQL, ClickHouse и Spark.
- `docker/postgres/init/01_raw_mock_data.sql` создает `raw_mock_data` и загружает все 10 CSV из `исходные данные`.
- `jobs/transform_to_postgres_star.py` строит модель звезда в PostgreSQL:
  - `dim_customer`
  - `dim_seller`
  - `dim_supplier`
  - `dim_product`
  - `dim_store`
  - `dim_date`
  - `fact_sales`
- `jobs/build_clickhouse_reports.py` создает 6 витрин в ClickHouse:
  - `mart_product_sales`
  - `mart_customer_sales`
  - `mart_time_sales`
  - `mart_store_sales`
  - `mart_supplier_sales`
  - `mart_product_quality`
- `sql/check_reports.sql` содержит базовые SQL-запросы для проверки результата.

## Запуск

Если нужно пересоздать базы с нуля:

```bash
docker compose down -v
```

Запустить обязательные сервисы:

```bash
docker compose up -d postgres clickhouse spark
```

После старта PostgreSQL автоматически загрузит 10000 строк из CSV в `raw_mock_data`.

Построить модель звезда в PostgreSQL:

```bash
docker compose run --rm spark /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-extra/postgresql-42.7.3.jar \
  /opt/spark/jobs/transform_to_postgres_star.py
```

Построить 6 отчетных таблиц в ClickHouse:

```bash
docker compose run --rm spark /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-extra/postgresql-42.7.3.jar,/opt/spark/jars-extra/clickhouse-jdbc-0.6.3-all.jar \
  /opt/spark/jobs/build_clickhouse_reports.py
```

## Проверка

PostgreSQL:

```bash
docker compose exec postgres psql -U postgres -d bigdataspark -c "SELECT count(*) FROM raw_mock_data;"
docker compose exec postgres psql -U postgres -d bigdataspark -c "SELECT count(*) FROM fact_sales;"
```

ClickHouse:

```bash
docker compose exec clickhouse clickhouse-client -q "SHOW TABLES LIKE 'mart_%'"
docker compose exec clickhouse clickhouse-client -q "SELECT count(*) FROM mart_product_sales"
docker compose exec clickhouse clickhouse-client -q "SELECT count(*) FROM mart_customer_sales"
docker compose exec clickhouse clickhouse-client -q "SELECT count(*) FROM mart_time_sales"
docker compose exec clickhouse clickhouse-client -q "SELECT count(*) FROM mart_store_sales"
docker compose exec clickhouse clickhouse-client -q "SELECT count(*) FROM mart_supplier_sales"
docker compose exec clickhouse clickhouse-client -q "SELECT count(*) FROM mart_product_quality"
```
