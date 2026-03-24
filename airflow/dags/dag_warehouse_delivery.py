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


def build_warehouse_delivery(**context):
    execution_date = context["execution_date"]
    business_date = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"Building warehouse_delivery for business_date={business_date}")

    conn = pymysql.connect(**DORIS_CONN)
    try:
        with conn.cursor() as cur:

            logger.info(f"Deleting existing data for {business_date}...")
            cur.execute("""
                DELETE FROM presentation.warehouse_delivery
                WHERE shipment_date = %s
            """, (business_date,))
            conn.commit()

            logger.info(f"Building warehouse_delivery for {business_date}...")
            cur.execute("""
                INSERT INTO presentation.warehouse_delivery (
                    shipment_date,
                    warehouse_id,
                    warehouse_name,
                    order_count,
                    total_shipment_qty,
                    delayed_orders_count,
                    unique_customers_count,
                    avg_processing_time_min
                )
                SELECT
                    CAST(ssd.dispatched_date AS DATE)               AS shipment_date,
                    hw.warehouse_code                               AS warehouse_id,
                    swd.warehouse_name                              AS warehouse_name,
                    COUNT(DISTINCT ssd.order_external_id)           AS order_count,
                    SUM(CAST(ssd.volume_cubic_cm AS DECIMAL))       AS total_shipment_qty,
                    -- Задержанные: actual > estimated
                    SUM(CASE
                        WHEN ssd.actual_delivery_date IS NOT NULL
                         AND ssd.estimated_delivery_date IS NOT NULL
                         AND CAST(ssd.actual_delivery_date AS DATE)
                           > CAST(ssd.estimated_delivery_date AS DATE)
                        THEN 1 ELSE 0
                    END)                                            AS delayed_orders_count,
                    -- Уникальные заказчики
                    COUNT(DISTINCT sod.user_external_id)            AS unique_customers_count,
                    -- Среднее время обработки в минутах
                    AVG(
                        TIMESTAMPDIFF(
                            MINUTE,
                            CAST(sod.order_date AS DATETIME),
                            CAST(ssd.dispatched_date AS DATETIME)
                        )
                    )                                               AS avg_processing_time_min
                FROM dwh_detailed.sat_shipment_details ssd
                -- Получаем warehouse через link
                JOIN dwh_detailed.lnk_shipment_warehouse lsw
                    ON ssd.hub_shipment_bk = lsw.hub_shipment_bk
                JOIN dwh_detailed.hub_warehouse hw
                    ON lsw.hub_warehouse_bk = hw.warehouse_code
                -- Детали склада
                LEFT JOIN dwh_detailed.sat_warehouse_details swd
                    ON hw.warehouse_code = swd.hub_warehouse_bk
                    AND swd.is_current = 1
                -- Детали заказа для получения user
                LEFT JOIN dwh_detailed.sat_order_details sod
                    ON ssd.order_external_id = sod.hub_order_bk
                    AND sod.is_current = 1
                WHERE
                    ssd.is_current = 1
                    AND CAST(ssd.dispatched_date AS DATE) = %s
                GROUP BY
                    CAST(ssd.dispatched_date AS DATE),
                    hw.warehouse_code,
                    swd.warehouse_name
            """, (business_date,))
            conn.commit()

            cur.execute("""
                SELECT COUNT(*) FROM presentation.warehouse_delivery
                WHERE shipment_date = %s
            """, (business_date,))
            cnt = cur.fetchone()[0]
            logger.info(f"warehouse_delivery built for {business_date}: {cnt} rows")

    finally:
        conn.close()


with DAG(
    dag_id="warehouse_delivery",
    description="Витрина 2: Доставка по складам",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 4 * * *", 
    catchup=False,
    tags=["presentation", "warehouse"],
) as dag:

    build_task = PythonOperator(
        task_id="build_warehouse_delivery",
        python_callable=build_warehouse_delivery,
        provide_context=True,
    )
