CREATE DATABASE IF NOT EXISTS presentation;

CREATE TABLE IF NOT EXISTS presentation.purchase_analytics (
    purchase_date           DATE            NOT NULL    COMMENT 'Дата закупки',
    product_id              VARCHAR(100)    NOT NULL    COMMENT 'ID товара (product_sku)',
    supplier_id             VARCHAR(100)                COMMENT 'ID поставщика (brand)',
    product_name            VARCHAR(255)                COMMENT 'Название товара',
    category                VARCHAR(100)                COMMENT 'Категория товара',
    supplier_name           VARCHAR(255)                COMMENT 'Название поставщика',
    purchase_qty            DECIMAL(18,2)               COMMENT 'Суммарное количество',
    total_purchase_amount   DECIMAL(18,2)               COMMENT 'Общая стоимость закупки',
    avg_unit_price          DECIMAL(18,2)               COMMENT 'Средняя цена'
)
UNIQUE KEY(purchase_date, product_id, supplier_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

CREATE TABLE IF NOT EXISTS presentation.warehouse_delivery (
    shipment_date           DATE            NOT NULL    COMMENT 'Дата отгрузки',
    warehouse_id            VARCHAR(100)    NOT NULL    COMMENT 'Код склада',
    warehouse_name          VARCHAR(255)                COMMENT 'Название склада',
    order_count             INT                         COMMENT 'Количество заказов',
    total_shipment_qty      DECIMAL(18,2)               COMMENT 'Суммарный объём продукции',
    delayed_orders_count    INT                         COMMENT 'Количество задержанных заказов',
    unique_customers_count  INT                         COMMENT 'Уникальные заказчики',
    avg_processing_time_min DECIMAL(18,2)               COMMENT 'Среднее время обработки (мин)'
)
UNIQUE KEY(shipment_date, warehouse_id)
DISTRIBUTED BY HASH(warehouse_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);
