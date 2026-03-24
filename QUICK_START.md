# Быстрый старт — ДЗ No3

## За 5 минут

```bash
# 1. Запустить весь стек
cd dwh_hw_3
docker-compose up -d

# 2. Подождать 3-5 минут, проверить статус
docker-compose ps

# 3. Настроить PostgreSQL для CDC (только первый раз!)
docker exec -it pg-master bash -c "
    psql -U postgres -c \"ALTER SYSTEM SET wal_level = logical;\"
    psql -U postgres -c \"ALTER SYSTEM SET max_replication_slots = 10;\"
    psql -U postgres -c \"ALTER SYSTEM SET max_wal_senders = 10;\"
    su -c \"pg_ctl restart -D \$PGDATA -m fast\" postgres
"

# 4. Перезапустить Debezium коннекторы
docker-compose restart debezium-init

# 5. Подождать еще 2 минуты для загрузки данных в DWH
```

## Доступы

| Сервис | URL | Логин/Пароль |
|--------|-----|--------------|
| Airflow | http://localhost:8888 | admin / admin |
| Metabase | http://localhost:3000 | (создать при первом входе) |
| Kafka UI | http://localhost:8090 | - |
| Doris FE | http://localhost:8030 | - |

## Проверка работы

### Шаг 1: Проверить данные в DDS

```bash
docker exec -it doris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "
USE dwh_detailed;
SELECT 'hub_user' as tbl, COUNT(*) as cnt FROM hub_user
UNION ALL SELECT 'hub_order', COUNT(*) FROM hub_order
UNION ALL SELECT 'hub_product', COUNT(*) FROM hub_product
UNION ALL SELECT 'hub_warehouse', COUNT(*) FROM hub_warehouse;
"
```

**Ожидается:** Ненулевые значения во всех таблицах.

### Шаг 2: Запустить DAG'и в Airflow

1. Открыть http://localhost:8888
2. Логин: `admin`, пароль: `admin`
3. Включить оба DAG'а (toggle справа):
   - `purchase_analytics`
   - `warehouse_delivery`
4. Нажать кнопку **Play** → **Trigger DAG** для каждого
5. Дождаться успешного выполнения (зеленые квадраты)

### Шаг 3: Проверить витрины

```bash
docker exec -it doris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "
USE presentation;
SELECT 'purchase_analytics' as tbl, COUNT(*) as cnt FROM purchase_analytics
UNION ALL SELECT 'warehouse_delivery', COUNT(*) FROM warehouse_delivery;
"
```

**Ожидается:** Ненулевые значения в обеих витринах.

### Шаг 4: Настроить Metabase

1. Открыть http://localhost:3000
2. **Первый вход:** Создать администратора (любое имя/email/пароль)
3. Skip рассылки
4. **Добавить подключение к БД:**
   - Database type: **MySQL**
   - Name: **Doris DWH**
   - Host: **doris-fe**
   - Port: **9030**
   - Database name: **presentation**
   - Username: **root**
   - Password: **(оставить пустым)**
   - Click **Save**

5. **Создать дашборды:**
   - New → Dashboard
   - Добавить Questions (SQL запросы к витринам)
   - Выбрать типы визуализаций

## Примеры SQL для дашбордов

### Dashboard 1: Аналитика закупок

#### 1. Динамика закупок по месяцам
```sql
SELECT 
    DATE_FORMAT(purchase_date, '%Y-%m') as month,
    SUM(total_purchase_amount) as total
FROM purchase_analytics
GROUP BY DATE_FORMAT(purchase_date, '%Y-%m')
ORDER BY month
```
Визуализация: **Line Chart** (X: month, Y: total)

#### 2. Топ-5 товаров
```sql
SELECT 
    product_name,
    SUM(purchase_qty) as qty
FROM purchase_analytics
GROUP BY product_name
ORDER BY qty DESC
LIMIT 5
```
Визуализация: **Bar Chart** (X: product_name, Y: qty)

#### 3. Доли категорий
```sql
SELECT 
    category,
    SUM(total_purchase_amount) as amount
FROM purchase_analytics
GROUP BY category
```
Визуализация: **Pie Chart** (Dimension: category, Metric: amount)

### Dashboard 2: Доставка по складам

#### 1. Заказы по складам
```sql
SELECT 
    warehouse_name,
    SUM(order_count) as orders
FROM warehouse_delivery
GROUP BY warehouse_name
ORDER BY orders DESC
```
Визуализация: **Bar Chart**

#### 2. Динамика времени обработки
```sql
SELECT 
    shipment_date,
    AVG(avg_processing_time_min) as avg_time
FROM warehouse_delivery
GROUP BY shipment_date
ORDER BY shipment_date
```
Визуализация: **Line Chart**

## Остановка и очистка

```bash
# Остановить все сервисы
docker-compose down

# Остановить и удалить все данные
docker-compose down -v
```

## Troubleshooting

### Проблема: DMP не загружает данные

```bash
# Проверить логи
docker logs dmp --tail=100

# Проверить Debezium коннекторы
curl http://localhost:8083/connectors?expand=status | python3 -m json.tool
```

### Проблема: Airflow DAG не запускается

```bash
# Проверить логи scheduler
docker logs airflow-scheduler --tail=100

# Проверить логи webserver
docker logs airflow-webserver --tail=100
```

### Проблема: Doris не отвечает

```bash
# Проверить статус FE
docker logs doris-fe --tail=100

# Проверить статус BE
docker logs doris-be --tail=100

# Может потребоваться больше памяти (минимум 8GB RAM)
```

### Проблема: Нет данных в исходных таблицах

```bash
# Загрузить mock-данные
python3 load_data.py
```

## Контакты

Если возникли проблемы, проверьте:
1. `TESTING_REPORT.md` — полный отчет о тестировании
2. `README.md` — подробная документация
3. Логи контейнеров через `docker logs <container_name>`
