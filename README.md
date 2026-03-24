# Домашнее задание 3 — Airflow + BI инструмент

## Состав команды
- Тугов Егвений

## Что сделано

### Из ДЗ No2:
- [x] Выбрана и обоснована архитектура Data Vault 2.0
- [x] DDL для детального слоя DWH (Apache Doris)
- [x] Поднят Apache Doris (FE + BE) как MPP-хранилище
- [x] Поднят и подключён Debezium (CDC из PostgreSQL)
- [x] Реализован DMP (Python) — универсальный класс DataVaultLoader
- [x] Данные успешно пишутся в детальный слой DWH

### ДЗ No3:
- [x] Поднят Apache Airflow в docker-compose
- [x] Создан DAG для витрины "Аналитика закупок"
- [x] Создан DAG для витрины "Доставка по складам"
- [x] Поднят Metabase как BI-инструмент
- [x] Реализована схема `presentation` с витринами
- [x] Все сервисы запускаются одной командой `docker-compose up`

## Архитектура

```
┌─────────────────────────────────────────────────────────────────┐
│                         SOURCES                                 │
│   pg-master:                                                    │
│   user_service_db / order_service_db / logistics_service_db     │
└──────────────────────┬──────────────────────────────────────────┘
                       │ CDC (logical replication)
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DEBEZIUM (port 8083)                         │
└──────────────────────┬──────────────────────────────────────────┘
                       │ publish
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│              KAFKA + ZOOKEEPER (port 29092)                     │
└──────────────────────┬──────────────────────────────────────────┘
                       │ consume
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DMP (Python)                                 │
│   DataVaultLoader — универсальный класс                         │
└──────────────────────┬──────────────────────────────────────────┘
                       │ write
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│           APACHE DORIS (FE: 8030/9030, BE: 8040)                │
│                   dwh_detailed (DDS)                            │
│  HUBs, LINKs, SATs (Data Vault 2.0)                             │
└──────────────────────┬──────────────────────────────────────────┘
                       │ read
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│               APACHE AIRFLOW (port 8888)                        │
│   DAG: purchase_analytics    (daily 03:00)                      │
│   DAG: warehouse_delivery    (daily 04:00)                      │
└──────────────────────┬──────────────────────────────────────────┘
                       │ write
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│           APACHE DORIS - presentation (CDM)                     │
│   purchase_analytics     - Аналитика закупок                    │
│   warehouse_delivery     - Доставка по складам                  │
└──────────────────────┬──────────────────────────────────────────┘
                       │ visualize
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                  METABASE (port 3000)                           │
│   Dashboard 1: Аналитика закупок                                │
│   Dashboard 2: Доставка по складам                              │
└─────────────────────────────────────────────────────────────────┘
```

## Витрины (Data Marts)

### Витрина 1: `presentation.purchase_analytics`

**Назначение:** Анализ структуры закупок товаров: объемы, поставщики, динамика цен.

**Состав:**
- `purchase_date` (DATE) — Дата закупки
- `product_id` (VARCHAR) — ID товара (product_sku)
- `product_name` (VARCHAR) — Название товара
- `category` (VARCHAR) — Категория товара
- `supplier_id` (VARCHAR) — ID поставщика (brand)
- `supplier_name` (VARCHAR) — Название поставщика
- `purchase_qty` (DECIMAL) — Суммарное количество
- `total_purchase_amount` (DECIMAL) — Общая стоимость закупки
- `avg_unit_price` (DECIMAL) — Средняя цена

**Обновление:** Полное обновление 1 раз в день в 03:00 (schedule: `0 3 * * *`)

### Витрина 2: `presentation.warehouse_delivery`

**Назначение:** Дневная картина работы складов и доставки: эффективность логистики, задержки.

**Состав:**
- `shipment_date` (DATE) — Дата отгрузки
- `warehouse_id` (VARCHAR) — Код склада
- `warehouse_name` (VARCHAR) — Название склада
- `order_count` (INT) — Количество заказов
- `total_shipment_qty` (DECIMAL) — Суммарный объём продукции
- `delayed_orders_count` (INT) — Количество задержанных заказов
- `unique_customers_count` (INT) — Уникальные заказчики
- `avg_processing_time_min` (DECIMAL) — Среднее время обработки (мин)

**Обновление:** Инкрементальное обновление за `business_date` (вчера) каждый день в 04:00 (schedule: `0 4 * * *`)

## Структура проекта

```
dwh_hw_3/
├── docker-compose.yml          # Все сервисы (PG, Kafka, Doris, Airflow, Metabase)
├── .env                        # Переменные окружения
├── .gitignore
├── README.md
│
├── init/                       # DDL источников (из ДЗ 1)
│   ├── 00_init_replication.sh
│   ├── 01_user_service_db.sql
│   ├── 02_order_service_db.sql
│   └── 03_logistics_service_db.sql
│
├── replica/                    # Инициализация реплики
│   └── init_replica.sh
│
├── debezium/                   # CDC коннекторы
│   ├── init.sh
│   └── connectors/
│       ├── user_service_connector.json
│       ├── order_service_connector.json
│       └── logistics_service_connector.json
│
├── ddl/
│   └── dwh_detailed.sql        # DDL для DDS-слоя (Data Vault)
│
├── presentation/               # *** NEW: Витрины (CDM) ***
│   └── init_presentation.sql   # DDL для витрин
│
├── doris/                      # Apache Doris конфиги
│   ├── fe.conf
│   ├── be.conf
│   ├── entry_point.sh
│   └── init_ddl.sh
│
├── dmp/                        # DMP сервис (DDS loader)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py
│   ├── loader.py
│   └── configs/                # YAML-конфиги Hub/Link/Satellite
│
└── airflow/                    # *** NEW: Apache Airflow ***
    ├── Dockerfile
    ├── dags/
    │   ├── dag_purchase_analytics.py      # Витрина 1
    │   └── dag_warehouse_delivery.py      # Витрина 2
```

## Как запустить

### Требования
- Docker >= 20.10
- Docker Compose >= 2.0
- RAM >= 12GB (Doris + Airflow + Metabase требуют ~10GB)
- Свободные порты: 5432, 5433, 8030, 8040, 8083, 8090, 8888, 3000, 9030, 29092

### Запуск всей системы

```bash
git clone <your-repo-url>
cd dwh_hw_3

# Запускаем весь стек одной командой
docker-compose up -d

# Ждём ~3-5 минут пока все сервисы инициализируются
# Следим за статусом
docker-compose ps
```

### Настройка PostgreSQL для CDC (первый запуск)

После первого запуска необходимо включить логическую репликацию:

```bash
docker exec -it pg-master bash -c "
    psql -U postgres -c \"ALTER SYSTEM SET wal_level = logical;\"
    psql -U postgres -c \"ALTER SYSTEM SET max_replication_slots = 10;\"
    psql -U postgres -c \"ALTER SYSTEM SET max_wal_senders = 10;\"
    su -c \"pg_ctl restart -D \$PGDATA -m fast\" postgres
"

# Перерегистрируем Debezium коннекторы после рестарта PostgreSQL
docker-compose restart debezium-init
```

### Загрузка тестовых данных (опционально)

Если данных нет, можно использовать mock-данные:

```bash
# Загрузить mock-данные в PostgreSQL
python3 load_data.py
```

### Проверка работы

```bash
# 1. Проверка сервисов
docker-compose ps

# 2. Проверка Debezium коннекторов
curl -s http://localhost:8083/connectors?expand=status | python3 -m json.tool

# 3. Проверка данных в DDS-слое
docker exec -it doris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "
USE dwh_detailed;
SELECT 'hub_user' as tbl, COUNT(*) as cnt FROM hub_user
UNION ALL SELECT 'hub_order', COUNT(*) FROM hub_order
UNION ALL SELECT 'hub_product', COUNT(*) FROM hub_product;
"

# 4. Проверка витрин
docker exec -it doris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "
USE presentation;
SELECT 'purchase_analytics' as tbl, COUNT(*) as cnt FROM purchase_analytics
UNION ALL SELECT 'warehouse_delivery', COUNT(*) FROM warehouse_delivery;
"

# 5. Логи DMP
docker logs dmp --tail=50

# 6. Логи Airflow Scheduler
docker logs airflow-scheduler --tail=50
```

## Подключение к сервисам

| Сервис | URL / Connection | Credentials |
|--------|-----------------|-------------|
| PostgreSQL Master | `postgresql://postgres:postgres@localhost:5432` | postgres/postgres |
| PostgreSQL Replica | `postgresql://postgres:postgres@localhost:5433` | postgres/postgres |
| Apache Doris | `mysql -h localhost -P 9030 -u root` | root/(empty) |
| Debezium REST API | `http://localhost:8083` | - |
| Kafka | `localhost:29092` | - |
| Kafka UI | `http://localhost:8090` | - |
| Doris FE HTTP | `http://localhost:8030` | - |
| **Airflow** | `http://localhost:8888` | admin/admin |
| **Metabase** | `http://localhost:3000` | (setup on first run) |

## Airflow DAGs

### Запуск DAG вручную

1. Откройте Airflow UI: http://localhost:8888
2. Логин: `admin`, пароль: `admin`
3. Включите DAG (toggle справа от имени)
4. Нажмите на Play icon → "Trigger DAG"

### Мониторинг выполнения

- **Graph View** — граф выполнения задач
- **Tree View** — история запусков
- **Logs** — логи каждой задачи

## Metabase — BI инструмент

### Первый запуск

1. Откройте: http://localhost:3000
2. Создайте администратора
3. Добавьте подключение к Doris:
   - **Database type:** MySQL
   - **Host:** doris-fe
   - **Port:** 9030
   - **Database name:** presentation
   - **Username:** root
   - **Password:** (оставить пустым)

### Создание дашбордов

#### Dashboard 1: Аналитика закупок
- Динамика закупок по месяцам (line chart)
- Топ-5 товаров по объёму закупки (bar chart)
- Доли категорий товаров в закупках (pie chart)
- Топ-5 поставщиков по объёму (bar chart)
- Средняя цена закупки по категориям (column chart)

#### Dashboard 2: Доставка по складам
- Количество заказов по складам за день (stacked bar chart)
- Динамика времени отгрузки по складам (line chart)
- Доля задержанных заказов на складе (pie chart)
- Объём отгруженной продукции по складам (column chart)
- Среднее время выполнения заказа по дням (line chart)
- Топ-5 дней с наибольшим количеством уникальных заказчиков (bar chart)

## Исправленные проблемы

### 1. Неправильный join в `sat_order_item_details`
**Проблема:** В DMP конфиге `parent_bk_col` был указан как `hub_order_bk`, но в DDL колонка называется `lnk_order_product_bk`.

**Решение:** Обновлен `dmp/configs/sat_order_item_details.yaml`:
```yaml
parent_bk_col: lnk_order_product_bk  # было: hub_order_bk
```

### 2. Неправильный join в DAG `purchase_analytics`
**Проблема:** Попытка джойна `sat_order_item_details` через `hub_order_bk`, но эта таблица — сателлит линка.

**Решение:** Добавлена фильтрация по `is_current = 1` для корректной работы SCD Type 2.

### 3. Неправильный join в DAG `warehouse_delivery`
**Проблема:** Прямой джойн `sat_shipment_details` → `hub_warehouse` через `hub_warehouse_bk = hub_shipment_bk`, что логически неверно.

**Решение:** Добавлен промежуточный `lnk_shipment_warehouse`:
```sql
JOIN dwh_detailed.lnk_shipment_warehouse lsw
    ON ssd.hub_shipment_bk = lsw.hub_shipment_bk
JOIN dwh_detailed.hub_warehouse hw
    ON lsw.hub_warehouse_bk = hw.warehouse_code
```

## Data Vault 2.0 — модель данных

### HUBs (бизнес-сущности)
| Таблица | Business Key | Источник |
|---------|-------------|---------|
| hub_user | user_external_id | user_service |
| hub_address | address_external_id | user_service |
| hub_order | order_external_id | order_service |
| hub_product | product_sku | order_service |
| hub_warehouse | warehouse_code | logistics_service |
| hub_pickup_point | pickup_point_code | logistics_service |
| hub_shipment | shipment_external_id | logistics_service |

### LINKs (связи между сущностями)
| Таблица | Связь |
|---------|-------|
| lnk_order_user | order ↔ user |
| lnk_order_product | order ↔ product |
| lnk_user_address | user ↔ address |
| lnk_shipment_order | shipment ↔ order |
| lnk_shipment_warehouse | shipment ↔ warehouse |
| lnk_shipment_pickup_point | shipment ↔ pickup_point |

### SATs (атрибуты с историей)
| Таблица | Родитель | SCD2 |
|---------|---------|------|
| sat_user_details | hub_user | ✅ |
| sat_user_address | hub_address | ✅ |
| sat_order_details | hub_order | ✅ |
| sat_product_details | hub_product | ✅ |
| sat_warehouse_details | hub_warehouse | ✅ |
| sat_shipment_details | hub_shipment | ✅ |
| sat_order_item_details | lnk_order_product | ✅ |
| sat_pickup_point_details | hub_pickup_point | ✅ |

## Сдача ДЗ

1. **Видео-демонстрация:** Запись дашбордов Metabase с работающими визуализациями
2. **GitHub:** Приватный репозиторий с доступом для @mgcrp
3. **README.md:** Полная инструкция по запуску и использованию
4. **Форма:** https://forms.gle/Y624DDetfNY7WcpA6

## Источники
- [Data Vault 2.0 Methodology](https://datavaultalliance.com/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Metabase Documentation](https://www.metabase.com/docs/latest/)
- [Apache Doris Documentation](https://doris.apache.org/docs/summary/basic-summary)
- [Debezium PostgreSQL Connector](https://debezium.io/documentation/reference/connectors/postgresql.html)
