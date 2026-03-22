-- ============================================================
-- DWH Detailed Layer - Data Vault 2.0
-- Apache Doris
-- Сгенерировано автоматически DDL Generator
-- ============================================================

CREATE DATABASE IF NOT EXISTS dwh_detailed;
USE dwh_detailed;

-- ============================================================
-- Source: logistics_service
-- ============================================================

-- HUB: hub_warehouse
CREATE TABLE IF NOT EXISTS hub_warehouse (
    hub_warehouse_id        BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    warehouse_code VARCHAR(100) NOT NULL        COMMENT 'Business key',
    load_dts                 DATETIME        NOT NULL                COMMENT 'Load timestamp',
    record_source            VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at               DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(hub_warehouse_id, warehouse_code)
DISTRIBUTED BY HASH(warehouse_code) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);
-- HUB: hub_pickup_point
CREATE TABLE IF NOT EXISTS hub_pickup_point (
    hub_pickup_point_id        BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    pickup_point_code VARCHAR(100) NOT NULL        COMMENT 'Business key',
    load_dts                 DATETIME        NOT NULL                COMMENT 'Load timestamp',
    record_source            VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at               DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(hub_pickup_point_id, pickup_point_code)
DISTRIBUTED BY HASH(pickup_point_code) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);
-- HUB: hub_shipment
CREATE TABLE IF NOT EXISTS hub_shipment (
    hub_shipment_id        BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    shipment_external_id VARCHAR(36) NOT NULL        COMMENT 'Business key',
    load_dts                 DATETIME        NOT NULL                COMMENT 'Load timestamp',
    record_source            VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at               DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(hub_shipment_id, shipment_external_id)
DISTRIBUTED BY HASH(shipment_external_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);
-- LINK: lnk_shipment_order
CREATE TABLE IF NOT EXISTS lnk_shipment_order (
    lnk_shipment_order_id  BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    hub_shipment_bk        VARCHAR(255)    NOT NULL                COMMENT 'BK from hub_shipment',
    hub_order_bk        VARCHAR(255)    NOT NULL                COMMENT 'BK from hub_order',
    load_dts            DATETIME        NOT NULL                COMMENT 'Load timestamp',
    record_source       VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(lnk_shipment_order_id, hub_shipment_bk)
DISTRIBUTED BY HASH(hub_shipment_bk) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- LINK: lnk_shipment_warehouse
CREATE TABLE IF NOT EXISTS lnk_shipment_warehouse (
    lnk_shipment_warehouse_id  BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    hub_shipment_bk        VARCHAR(255)    NOT NULL                COMMENT 'BK from hub_shipment',
    hub_warehouse_bk        VARCHAR(255)    NOT NULL                COMMENT 'BK from hub_warehouse',
    load_dts            DATETIME        NOT NULL                COMMENT 'Load timestamp',
    record_source       VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(lnk_shipment_warehouse_id, hub_shipment_bk)
DISTRIBUTED BY HASH(hub_shipment_bk) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- LINK: lnk_shipment_pickup_point
CREATE TABLE IF NOT EXISTS lnk_shipment_pickup_point (
    lnk_shipment_pickup_point_id  BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    hub_shipment_bk        VARCHAR(255)    NOT NULL                COMMENT 'BK from hub_shipment',
    hub_pickup_point_bk        VARCHAR(255)    NOT NULL                COMMENT 'BK from hub_pickup_point',
    load_dts            DATETIME        NOT NULL                COMMENT 'Load timestamp',
    record_source       VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(lnk_shipment_pickup_point_id, hub_shipment_bk)
DISTRIBUTED BY HASH(hub_shipment_bk) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- SATELLITE: sat_warehouse_details (parent: hub_warehouse)
CREATE TABLE IF NOT EXISTS sat_warehouse_details (
    sat_warehouse_details_id    BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    hub_warehouse_bk VARCHAR(255)    NOT NULL                COMMENT 'BK from parent hub_warehouse',
    warehouse_name     VARCHAR(255),
    warehouse_type     VARCHAR(50),
    country     VARCHAR(100),
    region     VARCHAR(100),
    city     VARCHAR(100),
    street_address     VARCHAR(255),
    postal_code     VARCHAR(20),
    is_active     BOOLEAN,
    max_capacity_cubic_meters     DECIMAL(18,2),
    operating_hours     VARCHAR(255),
    contact_phone     VARCHAR(50),
    manager_name     VARCHAR(255),
    load_dts             DATETIME        NOT NULL                COMMENT 'Load timestamp',
    load_end_dts         DATETIME                                COMMENT 'Load end timestamp (SCD2)',
    is_current           TINYINT         NOT NULL DEFAULT 1      COMMENT 'SCD2 current flag (1=current)',
    hash_diff            VARCHAR(64)     NOT NULL                COMMENT 'Hash of all attributes',
    record_source        VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(sat_warehouse_details_id, hub_warehouse_bk)
DISTRIBUTED BY HASH(hub_warehouse_bk) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- SATELLITE: sat_pickup_point_details (parent: hub_pickup_point)
CREATE TABLE IF NOT EXISTS sat_pickup_point_details (
    sat_pickup_point_details_id    BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    hub_pickup_point_bk VARCHAR(255)    NOT NULL                COMMENT 'BK from parent hub_pickup_point',
    pickup_point_name     VARCHAR(255),
    pickup_point_type     VARCHAR(50),
    country     VARCHAR(100),
    region     VARCHAR(100),
    city     VARCHAR(100),
    street_address     VARCHAR(255),
    postal_code     VARCHAR(20),
    is_active     BOOLEAN,
    max_capacity_packages     INT,
    operating_hours     VARCHAR(255),
    contact_phone     VARCHAR(50),
    partner_name     VARCHAR(255),
    load_dts             DATETIME        NOT NULL                COMMENT 'Load timestamp',
    load_end_dts         DATETIME                                COMMENT 'Load end timestamp (SCD2)',
    is_current           TINYINT         NOT NULL DEFAULT 1      COMMENT 'SCD2 current flag (1=current)',
    hash_diff            VARCHAR(64)     NOT NULL                COMMENT 'Hash of all attributes',
    record_source        VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(sat_pickup_point_details_id, hub_pickup_point_bk)
DISTRIBUTED BY HASH(hub_pickup_point_bk) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- SATELLITE: sat_shipment_details (parent: hub_shipment)
CREATE TABLE IF NOT EXISTS sat_shipment_details (
    sat_shipment_details_id    BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    hub_shipment_bk VARCHAR(255)    NOT NULL                COMMENT 'BK from parent hub_shipment',
    order_external_id     VARCHAR(36),
    tracking_number     VARCHAR(100),
    status     VARCHAR(50),
    weight_grams     INT,
    volume_cubic_cm     DECIMAL(18,2),
    package_count     INT,
    destination_type     VARCHAR(50),
    created_date     DATE,
    dispatched_date     DATE,
    estimated_delivery_date     DATE,
    actual_delivery_date     DATE,
    delivery_notes     STRING,
    recipient_name     VARCHAR(255),
    load_dts             DATETIME        NOT NULL                COMMENT 'Load timestamp',
    load_end_dts         DATETIME                                COMMENT 'Load end timestamp (SCD2)',
    is_current           TINYINT         NOT NULL DEFAULT 1      COMMENT 'SCD2 current flag (1=current)',
    hash_diff            VARCHAR(64)     NOT NULL                COMMENT 'Hash of all attributes',
    record_source        VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(sat_shipment_details_id, hub_shipment_bk)
DISTRIBUTED BY HASH(hub_shipment_bk) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- ============================================================
-- Source: order_service
-- ============================================================

-- HUB: hub_order
CREATE TABLE IF NOT EXISTS hub_order (
    hub_order_id        BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    order_external_id VARCHAR(36) NOT NULL        COMMENT 'Business key',
    load_dts                 DATETIME        NOT NULL                COMMENT 'Load timestamp',
    record_source            VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at               DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(hub_order_id, order_external_id)
DISTRIBUTED BY HASH(order_external_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);
-- HUB: hub_product
CREATE TABLE IF NOT EXISTS hub_product (
    hub_product_id        BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    product_sku VARCHAR(100) NOT NULL        COMMENT 'Business key',
    load_dts                 DATETIME        NOT NULL                COMMENT 'Load timestamp',
    record_source            VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at               DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(hub_product_id, product_sku)
DISTRIBUTED BY HASH(product_sku) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);
-- LINK: lnk_order_user
CREATE TABLE IF NOT EXISTS lnk_order_user (
    lnk_order_user_id  BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    hub_order_bk        VARCHAR(255)    NOT NULL                COMMENT 'BK from hub_order',
    hub_user_bk        VARCHAR(255)    NOT NULL                COMMENT 'BK from hub_user',
    load_dts            DATETIME        NOT NULL                COMMENT 'Load timestamp',
    record_source       VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(lnk_order_user_id, hub_order_bk)
DISTRIBUTED BY HASH(hub_order_bk) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- LINK: lnk_order_product
CREATE TABLE IF NOT EXISTS lnk_order_product (
    lnk_order_product_id  BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    hub_order_bk        VARCHAR(255)    NOT NULL                COMMENT 'BK from hub_order',
    hub_product_bk        VARCHAR(255)    NOT NULL                COMMENT 'BK from hub_product',
    load_dts            DATETIME        NOT NULL                COMMENT 'Load timestamp',
    record_source       VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(lnk_order_product_id, hub_order_bk)
DISTRIBUTED BY HASH(hub_order_bk) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- SATELLITE: sat_order_details (parent: hub_order)
CREATE TABLE IF NOT EXISTS sat_order_details (
    sat_order_details_id    BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    hub_order_bk VARCHAR(255)    NOT NULL                COMMENT 'BK from parent hub_order',
    user_external_id     VARCHAR(36),
    order_number     VARCHAR(100),
    order_date     DATE,
    status     VARCHAR(50),
    subtotal     DECIMAL(18,2),
    tax_amount     DECIMAL(18,2),
    shipping_cost     DECIMAL(18,2),
    discount_amount     DECIMAL(18,2),
    currency     VARCHAR(10),
    delivery_type     VARCHAR(50),
    expected_delivery_date     DATE,
    actual_delivery_date     DATE,
    payment_method     VARCHAR(50),
    payment_status     VARCHAR(50),
    load_dts             DATETIME        NOT NULL                COMMENT 'Load timestamp',
    load_end_dts         DATETIME                                COMMENT 'Load end timestamp (SCD2)',
    is_current           TINYINT         NOT NULL DEFAULT 1      COMMENT 'SCD2 current flag (1=current)',
    hash_diff            VARCHAR(64)     NOT NULL                COMMENT 'Hash of all attributes',
    record_source        VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(sat_order_details_id, hub_order_bk)
DISTRIBUTED BY HASH(hub_order_bk) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- SATELLITE: sat_product_details (parent: hub_product)
CREATE TABLE IF NOT EXISTS sat_product_details (
    sat_product_details_id    BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    hub_product_bk VARCHAR(255)    NOT NULL                COMMENT 'BK from parent hub_product',
    product_name     VARCHAR(255),
    category     VARCHAR(100),
    brand     VARCHAR(100),
    price     DECIMAL(18,2),
    currency     VARCHAR(10),
    weight_grams     INT,
    dimensions_length_cm     DECIMAL(10,2),
    dimensions_width_cm     DECIMAL(10,2),
    dimensions_height_cm     DECIMAL(10,2),
    is_active     BOOLEAN,
    load_dts             DATETIME        NOT NULL                COMMENT 'Load timestamp',
    load_end_dts         DATETIME                                COMMENT 'Load end timestamp (SCD2)',
    is_current           TINYINT         NOT NULL DEFAULT 1      COMMENT 'SCD2 current flag (1=current)',
    hash_diff            VARCHAR(64)     NOT NULL                COMMENT 'Hash of all attributes',
    record_source        VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(sat_product_details_id, hub_product_bk)
DISTRIBUTED BY HASH(hub_product_bk) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- SATELLITE: sat_order_item_details (parent: lnk_order_product)
CREATE TABLE IF NOT EXISTS sat_order_item_details (
    sat_order_item_details_id    BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    lnk_order_product_bk VARCHAR(255)    NOT NULL                COMMENT 'BK from parent lnk_order_product',
    quantity     INT,
    unit_price     DECIMAL(18,2),
    total_price     DECIMAL(18,2),
    product_name_snapshot     VARCHAR(255),
    product_category_snapshot     VARCHAR(100),
    product_brand_snapshot     VARCHAR(100),
    load_dts             DATETIME        NOT NULL                COMMENT 'Load timestamp',
    load_end_dts         DATETIME                                COMMENT 'Load end timestamp (SCD2)',
    is_current           TINYINT         NOT NULL DEFAULT 1      COMMENT 'SCD2 current flag (1=current)',
    hash_diff            VARCHAR(64)     NOT NULL                COMMENT 'Hash of all attributes',
    record_source        VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(sat_order_item_details_id, lnk_order_product_bk)
DISTRIBUTED BY HASH(lnk_order_product_bk) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- ============================================================
-- Source: user_service
-- ============================================================

-- HUB: hub_user
CREATE TABLE IF NOT EXISTS hub_user (
    hub_user_id        BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    user_external_id VARCHAR(36) NOT NULL        COMMENT 'Business key',
    load_dts                 DATETIME        NOT NULL                COMMENT 'Load timestamp',
    record_source            VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at               DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(hub_user_id, user_external_id)
DISTRIBUTED BY HASH(user_external_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);
-- HUB: hub_address
CREATE TABLE IF NOT EXISTS hub_address (
    hub_address_id        BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    address_external_id VARCHAR(36) NOT NULL        COMMENT 'Business key',
    load_dts                 DATETIME        NOT NULL                COMMENT 'Load timestamp',
    record_source            VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at               DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(hub_address_id, address_external_id)
DISTRIBUTED BY HASH(address_external_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);
-- LINK: lnk_user_address
CREATE TABLE IF NOT EXISTS lnk_user_address (
    lnk_user_address_id  BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    hub_user_bk        VARCHAR(255)    NOT NULL                COMMENT 'BK from hub_user',
    hub_address_bk        VARCHAR(255)    NOT NULL                COMMENT 'BK from hub_address',
    load_dts            DATETIME        NOT NULL                COMMENT 'Load timestamp',
    record_source       VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(lnk_user_address_id, hub_user_bk)
DISTRIBUTED BY HASH(hub_user_bk) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- SATELLITE: sat_user_details (parent: hub_user)
CREATE TABLE IF NOT EXISTS sat_user_details (
    sat_user_details_id    BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    hub_user_bk VARCHAR(255)    NOT NULL                COMMENT 'BK from parent hub_user',
    email     VARCHAR(255),
    first_name     VARCHAR(255),
    last_name     VARCHAR(255),
    phone     VARCHAR(50),
    date_of_birth     DATE,
    registration_date     DATE,
    status     VARCHAR(50),
    load_dts             DATETIME        NOT NULL                COMMENT 'Load timestamp',
    load_end_dts         DATETIME                                COMMENT 'Load end timestamp (SCD2)',
    is_current           TINYINT         NOT NULL DEFAULT 1      COMMENT 'SCD2 current flag (1=current)',
    hash_diff            VARCHAR(64)     NOT NULL                COMMENT 'Hash of all attributes',
    record_source        VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(sat_user_details_id, hub_user_bk)
DISTRIBUTED BY HASH(hub_user_bk) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- SATELLITE: sat_user_address (parent: hub_address)
CREATE TABLE IF NOT EXISTS sat_user_address (
    sat_user_address_id    BIGINT          NOT NULL AUTO_INCREMENT COMMENT 'Surrogate key',
    hub_address_bk VARCHAR(255)    NOT NULL                COMMENT 'BK from parent hub_address',
    user_external_id     VARCHAR(36),
    address_type     VARCHAR(50),
    country     VARCHAR(100),
    region     VARCHAR(100),
    city     VARCHAR(100),
    street_address     VARCHAR(255),
    postal_code     VARCHAR(20),
    apartment     VARCHAR(50),
    is_default     BOOLEAN,
    load_dts             DATETIME        NOT NULL                COMMENT 'Load timestamp',
    load_end_dts         DATETIME                                COMMENT 'Load end timestamp (SCD2)',
    is_current           TINYINT         NOT NULL DEFAULT 1      COMMENT 'SCD2 current flag (1=current)',
    hash_diff            VARCHAR(64)     NOT NULL                COMMENT 'Hash of all attributes',
    record_source        VARCHAR(255)    NOT NULL                COMMENT 'Source system',
    created_at           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created timestamp'
)
UNIQUE KEY(sat_user_address_id, hub_address_bk)
DISTRIBUTED BY HASH(hub_address_bk) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);
