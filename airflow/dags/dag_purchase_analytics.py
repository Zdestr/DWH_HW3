from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pymysql
import logging

logger = logging.getLogger(__name__)

DORIS_CONN = {
    "host": "doris-fe",
    "port": 9030,
    "user": "root",
    "password": "",
    "charset": "utf8mb4",
}

DEFAULT_ARGS = {
    "owner": "dwh",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}


def build_purchase_analytics(**context):

    conn = pymysql.connect(**DORIS_CONN)
    try:
        with conn.cursor() as cur:
            logger.info("Truncating presentation.purchase_analytics...")
            cur.execute("TRUNCATE TABLE presentation.purchase_analytics")
            conn.commit()

            logger.info("Building purchase_analytics...")
            cur.execute("""
                INSERT INTO presentation.purchase_analytics (
                    purchase_date,
                    product_id,
                    product_name,
                    category,
                    supplier_id,
                    supplier_name,
                    purchase_qty,
                    total_purchase_amount,
                    avg_unit_price
                )
                SELECT
                    CAST(sod.order_date AS DATE)            AS purchase_date,
                    hp.product_sku                          AS product_id,
                    spd.product_name                        AS product_name,
                    spd.category                            AS category,
                    COALESCE(spd.brand, 'unknown')          AS supplier_id,
                    COALESCE(spd.brand, 'unknown')          AS supplier_name,
                    SUM(CAST(soid.quantity AS DECIMAL))     AS purchase_qty,
                    SUM(CAST(soid.total_price AS DECIMAL))  AS total_purchase_amount,
                    AVG(CAST(soid.unit_price AS DECIMAL))   AS avg_unit_price
                FROM dwh_detailed.lnk_order_product lop
                -- Получаем order_date
                JOIN dwh_detailed.sat_order_details sod
                    ON lop.hub_order_bk = sod.hub_order_bk
                    AND sod.is_current = 1
                -- Получаем детали товара
                JOIN dwh_detailed.hub_product hp
                    ON lop.hub_product_bk = hp.product_sku
                JOIN dwh_detailed.sat_product_details spd
                    ON hp.product_sku = spd.hub_product_bk
                    AND spd.is_current = 1
                -- Получаем детали позиций заказа (join by order_external_id)
                JOIN dwh_detailed.sat_order_item_details soid
                    ON lop.hub_order_bk = soid.lnk_order_product_bk
                    AND soid.is_current = 1
                WHERE
                    sod.order_date IS NOT NULL
                GROUP BY
                    CAST(sod.order_date AS DATE),
                    hp.product_sku,
                    spd.product_name,
                    spd.category,
                    spd.brand
            """)
            conn.commit()

            cur.execute("SELECT COUNT(*) FROM presentation.purchase_analytics")
            cnt = cur.fetchone()[0]
            logger.info(f"purchase_analytics built: {cnt} rows")

    finally:
        conn.close()


with DAG(
    dag_id="purchase_analytics",
    description="Витрина 1: Аналитика закупок",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 3 * * *",
    catchup=False,
    tags=["presentation", "purchase"],
) as dag:

    build_task = PythonOperator(
        task_id="build_purchase_analytics",
        python_callable=build_purchase_analytics,
        provide_context=True,
    )
