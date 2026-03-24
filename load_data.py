#!/usr/bin/env python3
"""
Скрипт загрузки тестовых данных в PostgreSQL
"""
import csv
import psycopg2
from datetime import datetime
import os

def get_conn(dbname):
    return psycopg2.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="postgres",
        dbname=dbname
    )

def clean_val(val):
    if val == '' or val is None:
        return None
    if val == 'True':
        return True
    if val == 'False':
        return False
    return val

DATA_DIR = os.path.expanduser("~/dwh_hw_3/mock_data_extracted/mock_data")

# ============================================================
# user_service_db
# ============================================================
def load_users():
    conn = get_conn("user_service_db")
    cur = conn.cursor()
    print("Loading users...")
    with open(f"{DATA_DIR}/user_service_users.csv") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            batch.append((
                clean_val(row['user_external_id']),
                clean_val(row['email']),
                clean_val(row['first_name']),
                clean_val(row['last_name']),
                clean_val(row['phone']),
                clean_val(row['date_of_birth']),
                clean_val(row['registration_date']),
                clean_val(row['status']),
                clean_val(row['effective_from']),
                clean_val(row['effective_to']),
                clean_val(row['is_current']),
                clean_val(row['created_at']),
                clean_val(row['updated_at']),
                clean_val(row['created_by']),
                clean_val(row['updated_by']),
            ))
            if len(batch) >= 1000:
                cur.executemany("""
                    INSERT INTO users (
                        user_external_id, email, first_name, last_name,
                        phone, date_of_birth, registration_date, status,
                        effective_from, effective_to, is_current,
                        created_at, updated_at, created_by, updated_by
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (user_external_id) DO NOTHING
                """, batch)
                conn.commit()
                batch = []
        if batch:
            cur.executemany("""
                INSERT INTO users (
                    user_external_id, email, first_name, last_name,
                    phone, date_of_birth, registration_date, status,
                    effective_from, effective_to, is_current,
                    created_at, updated_at, created_by, updated_by
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (user_external_id) DO NOTHING
            """, batch)
            conn.commit()
    print(f"Users loaded!")
    cur.close()
    conn.close()

def load_user_addresses():
    conn = get_conn("user_service_db")
    cur = conn.cursor()
    print("Loading user_addresses...")
    with open(f"{DATA_DIR}/user_service_user_addresses.csv") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            batch.append((
                clean_val(row['address_external_id']),
                clean_val(row['user_external_id']),
                clean_val(row['address_type']),
                clean_val(row['country']),
                clean_val(row['region']),
                clean_val(row['city']),
                clean_val(row['street_address']),
                clean_val(row['postal_code']),
                clean_val(row['apartment']),
                clean_val(row['is_default']),
                clean_val(row['effective_from']),
                clean_val(row['effective_to']),
                clean_val(row['is_current']),
                clean_val(row['created_at']),
                clean_val(row['updated_at']),
                clean_val(row['created_by']),
                clean_val(row['updated_by']),
            ))
            if len(batch) >= 1000:
                cur.executemany("""
                    INSERT INTO user_addresses (
                        address_external_id, user_external_id, address_type,
                        country, region, city, street_address, postal_code,
                        apartment, is_default, effective_from, effective_to,
                        is_current, created_at, updated_at, created_by, updated_by
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (address_external_id) DO NOTHING
                """, batch)
                conn.commit()
                batch = []
        if batch:
            cur.executemany("""
                INSERT INTO user_addresses (
                    address_external_id, user_external_id, address_type,
                    country, region, city, street_address, postal_code,
                    apartment, is_default, effective_from, effective_to,
                    is_current, created_at, updated_at, created_by, updated_by
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (address_external_id) DO NOTHING
            """, batch)
            conn.commit()
    print("User addresses loaded!")
    cur.close()
    conn.close()

# ============================================================
# order_service_db
# ============================================================
def load_products():
    conn = get_conn("order_service_db")
    cur = conn.cursor()
    print("Loading products...")
    with open(f"{DATA_DIR}/order_service_products.csv") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            batch.append((
                clean_val(row['product_sku']),
                clean_val(row['product_name']),
                clean_val(row['category']),
                clean_val(row['brand']),
                clean_val(row['price']),
                clean_val(row['currency']),
                clean_val(row['weight_grams']),
                clean_val(row['dimensions_length_cm']),
                clean_val(row['dimensions_width_cm']),
                clean_val(row['dimensions_height_cm']),
                clean_val(row['is_active']),
                clean_val(row['effective_from']),
                clean_val(row['effective_to']),
                clean_val(row['is_current']),
                clean_val(row['created_at']),
                clean_val(row['updated_at']),
                clean_val(row['created_by']),
                clean_val(row['updated_by']),
            ))
            if len(batch) >= 1000:
                cur.executemany("""
                    INSERT INTO products (
                        product_sku, product_name, category, brand,
                        price, currency, weight_grams,
                        dimensions_length_cm, dimensions_width_cm, dimensions_height_cm,
                        is_active, effective_from, effective_to, is_current,
                        created_at, updated_at, created_by, updated_by
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (product_sku) DO NOTHING
                """, batch)
                conn.commit()
                batch = []
        if batch:
            cur.executemany("""
                INSERT INTO products (
                    product_sku, product_name, category, brand,
                    price, currency, weight_grams,
                    dimensions_length_cm, dimensions_width_cm, dimensions_height_cm,
                    is_active, effective_from, effective_to, is_current,
                    created_at, updated_at, created_by, updated_by
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (product_sku) DO NOTHING
            """, batch)
            conn.commit()
    print("Products loaded!")
    cur.close()
    conn.close()

def load_orders():
    conn = get_conn("order_service_db")
    cur = conn.cursor()
    print("Loading orders...")
    with open(f"{DATA_DIR}/order_service_orders.csv") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            batch.append((
                clean_val(row['order_external_id']),
                clean_val(row['user_external_id']),
                clean_val(row['order_number']),
                clean_val(row['order_date']),
                clean_val(row['status']),
                clean_val(row['subtotal']),
                clean_val(row['tax_amount']),
                clean_val(row['shipping_cost']),
                clean_val(row['discount_amount']),
                clean_val(row['currency']),
                clean_val(row['delivery_address_external_id']),
                clean_val(row['delivery_type']),
                clean_val(row['expected_delivery_date']),
                clean_val(row['actual_delivery_date']),
                clean_val(row['payment_method']),
                clean_val(row['payment_status']),
                clean_val(row['effective_from']),
                clean_val(row['effective_to']),
                clean_val(row['is_current']),
                clean_val(row['created_at']),
                clean_val(row['updated_at']),
                clean_val(row['created_by']),
                clean_val(row['updated_by']),
            ))
            if len(batch) >= 1000:
                cur.executemany("""
                    INSERT INTO orders (
                        order_external_id, user_external_id, order_number,
                        order_date, status, subtotal, tax_amount,
                        shipping_cost, discount_amount, currency,
                        delivery_address_external_id, delivery_type,
                        expected_delivery_date, actual_delivery_date,
                        payment_method, payment_status,
                        effective_from, effective_to, is_current,
                        created_at, updated_at, created_by, updated_by
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (order_external_id) DO NOTHING
                """, batch)
                conn.commit()
                batch = []
        if batch:
            cur.executemany("""
                INSERT INTO orders (
                    order_external_id, user_external_id, order_number,
                    order_date, status, subtotal, tax_amount,
                    shipping_cost, discount_amount, currency,
                    delivery_address_external_id, delivery_type,
                    expected_delivery_date, actual_delivery_date,
                    payment_method, payment_status,
                    effective_from, effective_to, is_current,
                    created_at, updated_at, created_by, updated_by
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (order_external_id) DO NOTHING
            """, batch)
            conn.commit()
    print("Orders loaded!")
    cur.close()
    conn.close()

def load_order_items():
    conn = get_conn("order_service_db")
    cur = conn.cursor()
    print("Loading order_items...")
    with open(f"{DATA_DIR}/order_service_order_items.csv") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            batch.append((
                clean_val(row['order_external_id']),
                clean_val(row['product_sku']),
                clean_val(row['quantity']),
                clean_val(row['unit_price']),
                clean_val(row['total_price']),
                clean_val(row['product_name_snapshot']),
                clean_val(row['product_category_snapshot']),
                clean_val(row['product_brand_snapshot']),
                clean_val(row['created_at']),
                clean_val(row['updated_at']),
                clean_val(row['created_by']),
                clean_val(row['updated_by']),
            ))
            if len(batch) >= 1000:
                cur.executemany("""
                    INSERT INTO order_items (
                        order_external_id, product_sku, quantity,
                        unit_price, total_price,
                        product_name_snapshot, product_category_snapshot,
                        product_brand_snapshot,
                        created_at, updated_at, created_by, updated_by
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, batch)
                conn.commit()
                batch = []
        if batch:
            cur.executemany("""
                INSERT INTO order_items (
                    order_external_id, product_sku, quantity,
                    unit_price, total_price,
                    product_name_snapshot, product_category_snapshot,
                    product_brand_snapshot,
                    created_at, updated_at, created_by, updated_by
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, batch)
            conn.commit()
    print("Order items loaded!")
    cur.close()
    conn.close()

# ============================================================
# logistics_service_db
# ============================================================
def load_warehouses():
    conn = get_conn("logistics_service_db")
    cur = conn.cursor()
    print("Loading warehouses...")
    with open(f"{DATA_DIR}/logistics_service_warehouses.csv") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            batch.append((
                clean_val(row['warehouse_code']),
                clean_val(row['warehouse_name']),
                clean_val(row['warehouse_type']),
                clean_val(row['country']),
                clean_val(row['region']),
                clean_val(row['city']),
                clean_val(row['street_address']),
                clean_val(row['postal_code']),
                clean_val(row['is_active']),
                clean_val(row['max_capacity_cubic_meters']),
                clean_val(row['operating_hours']),
                clean_val(row['contact_phone']),
                clean_val(row['manager_name']),
                clean_val(row['effective_from']),
                clean_val(row['effective_to']),
                clean_val(row['is_current']),
                clean_val(row['created_at']),
                clean_val(row['updated_at']),
                clean_val(row['created_by']),
                clean_val(row['updated_by']),
            ))
            if len(batch) >= 1000:
                cur.executemany("""
                    INSERT INTO warehouses (
                        warehouse_code, warehouse_name, warehouse_type,
                        country, region, city, street_address, postal_code,
                        is_active, max_capacity_cubic_meters, operating_hours,
                        contact_phone, manager_name,
                        effective_from, effective_to, is_current,
                        created_at, updated_at, created_by, updated_by
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (warehouse_code) DO NOTHING
                """, batch)
                conn.commit()
                batch = []
        if batch:
            cur.executemany("""
                INSERT INTO warehouses (
                    warehouse_code, warehouse_name, warehouse_type,
                    country, region, city, street_address, postal_code,
                    is_active, max_capacity_cubic_meters, operating_hours,
                    contact_phone, manager_name,
                    effective_from, effective_to, is_current,
                    created_at, updated_at, created_by, updated_by
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (warehouse_code) DO NOTHING
            """, batch)
            conn.commit()
    print("Warehouses loaded!")
    cur.close()
    conn.close()

def load_pickup_points():
    conn = get_conn("logistics_service_db")
    cur = conn.cursor()
    print("Loading pickup_points...")
    with open(f"{DATA_DIR}/logistics_service_pickup_points.csv") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            batch.append((
                clean_val(row['pickup_point_code']),
                clean_val(row['pickup_point_name']),
                clean_val(row['pickup_point_type']),
                clean_val(row['country']),
                clean_val(row['region']),
                clean_val(row['city']),
                clean_val(row['street_address']),
                clean_val(row['postal_code']),
                clean_val(row['is_active']),
                clean_val(row['max_capacity_packages']),
                clean_val(row['operating_hours']),
                clean_val(row['contact_phone']),
                clean_val(row['partner_name']),
                clean_val(row['effective_from']),
                clean_val(row['effective_to']),
                clean_val(row['is_current']),
                clean_val(row['created_at']),
                clean_val(row['updated_at']),
                clean_val(row['created_by']),
                clean_val(row['updated_by']),
            ))
            if len(batch) >= 1000:
                cur.executemany("""
                    INSERT INTO pickup_points (
                        pickup_point_code, pickup_point_name, pickup_point_type,
                        country, region, city, street_address, postal_code,
                        is_active, max_capacity_packages, operating_hours,
                        contact_phone, partner_name,
                        effective_from, effective_to, is_current,
                        created_at, updated_at, created_by, updated_by
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (pickup_point_code) DO NOTHING
                """, batch)
                conn.commit()
                batch = []
        if batch:
            cur.executemany("""
                INSERT INTO pickup_points (
                    pickup_point_code, pickup_point_name, pickup_point_type,
                    country, region, city, street_address, postal_code,
                    is_active, max_capacity_packages, operating_hours,
                    contact_phone, partner_name,
                    effective_from, effective_to, is_current,
                    created_at, updated_at, created_by, updated_by
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (pickup_point_code) DO NOTHING
            """, batch)
            conn.commit()
    print("Pickup points loaded!")
    cur.close()
    conn.close()

def load_shipments():
    conn = get_conn("logistics_service_db")
    cur = conn.cursor()
    print("Loading shipments...")
    with open(f"{DATA_DIR}/logistics_service_shipments.csv") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            batch.append((
                clean_val(row['shipment_external_id']),
                clean_val(row['order_external_id']),
                clean_val(row['tracking_number']),
                clean_val(row['status']),
                clean_val(row['weight_grams']),
                clean_val(row['volume_cubic_cm']),
                clean_val(row['package_count']),
                clean_val(row['origin_warehouse_code']),
                clean_val(row['destination_type']),
                clean_val(row['destination_pickup_point_code']),
                clean_val(row['destination_address_external_id']),
                clean_val(row['created_date']),
                clean_val(row['dispatched_date']),
                clean_val(row['estimated_delivery_date']),
                clean_val(row['actual_delivery_date']),
                clean_val(row['delivery_notes']),
                clean_val(row['recipient_name']),
                clean_val(row['delivery_signature']),
                clean_val(row['effective_from']),
                clean_val(row['effective_to']),
                clean_val(row['is_current']),
                clean_val(row['created_at']),
                clean_val(row['updated_at']),
                clean_val(row['created_by']),
                clean_val(row['updated_by']),
            ))
            if len(batch) >= 1000:
                cur.executemany("""
                    INSERT INTO shipments (
                        shipment_external_id, order_external_id, tracking_number,
                        status, weight_grams, volume_cubic_cm, package_count,
                        origin_warehouse_code, destination_type,
                        destination_pickup_point_code, destination_address_external_id,
                        created_date, dispatched_date, estimated_delivery_date,
                        actual_delivery_date, delivery_notes, recipient_name,
                        delivery_signature, effective_from, effective_to, is_current,
                        created_at, updated_at, created_by, updated_by
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (shipment_external_id) DO NOTHING
                """, batch)
                conn.commit()
                batch = []
        if batch:
            cur.executemany("""
                INSERT INTO shipments (
                    shipment_external_id, order_external_id, tracking_number,
                    status, weight_grams, volume_cubic_cm, package_count,
                    origin_warehouse_code, destination_type,
                    destination_pickup_point_code, destination_address_external_id,
                    created_date, dispatched_date, estimated_delivery_date,
                    actual_delivery_date, delivery_notes, recipient_name,
                    delivery_signature, effective_from, effective_to, is_current,
                    created_at, updated_at, created_by, updated_by
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (shipment_external_id) DO NOTHING
            """, batch)
            conn.commit()
    print("Shipments loaded!")
    cur.close()
    conn.close()

if __name__ == "__main__":
    print("=== Loading test data ===")
    load_users()
    load_user_addresses()
    load_products()
    load_orders()
    load_order_items()
    load_warehouses()
    load_pickup_points()
    load_shipments()
    print("=== All data loaded! ===")
