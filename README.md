
```markdown
# Домашнее задание 2 — DWH на Data Vault 2.0 + Apache Doris

## Состав команды
- [Ваше имя]

## Что сделано
- [x] Выбрана и обоснована архитектура Data Vault 2.0
- [x] DDL для детального слоя DWH (Apache Doris)
- [x] ER-диаграмма детального слоя
- [x] Поднят Apache Doris (FE + BE) как MPP-хранилище
- [x] Поднят и подключён Debezium (CDC из PostgreSQL)
- [x] Реализован DMP (Python) — универсальный класс DataVaultLoader
- [x] Данные успешно пишутся в детальный слой DWH
- [x] Бонус: Генератор DDL из YAML-конфигов
- [x] Бонус: MPP-база Apache Doris вместо PostgreSQL
- [x] Бонус: Универсальный класс + YAML-конфиги

## Обоснование архитектуры

### Почему Data Vault 2.0?
- **Гибкость**: легко добавлять новые источники без изменения существующей структуры
- **Историчность**: SCD Type 2 из коробки в satellites
- **Аудитируемость**: все изменения трекаются через load_dts / load_end_dts
- **Масштабируемость**: хорошо параллелизуется в MPP

### Почему Apache Doris?
- Колоночное хранение → быстрые аналитические запросы
- Поддержка стандартного SQL и JOIN (критично для Data Vault)
- MySQL-совместимый протокол → простая интеграция
- Горизонтальное масштабирование через FE/BE архитектуру
- Значительно быстрее PostgreSQL на аналитических запросах (OLAP)

## Архитектура

┌─────────────────────────────────────────────────────────────────┐
│                         SOURCES                                 │
│   pg-master:                                                    │
│   user_service_db / order_service_db / logistics_service_db     │
└──────────────────────┬──────────────────────────────────────────┘
                       │ CDC (logical replication)
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DEBEZIUM (port 8083)                         │
│   user_service_connector                                        │
│   order_service_connector                                       │
│   logistics_service_connector                                   │
└──────────────────────┬──────────────────────────────────────────┘
                       │ publish
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│              KAFKA + ZOOKEEPER (port 29092)                     │
│   user_service.public.users                                     │
│   user_service.public.user_addresses                            │
│   order_service.public.orders                                   │
│   order_service.public.products                                 │
│   order_service.public.order_items                              │
│   logistics_service.public.warehouses                           │
│   logistics_service.public.pickup_points                        │
│   logistics_service.public.shipments                            │
└──────────────────────┬──────────────────────────────────────────┘
                       │ consume
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DMP (Python)                                 │
│   DataVaultLoader — универсальный класс                         │
│   YAML-конфиги для каждой таблицы                               │
│   Поддержка Hub / Link / Satellite                              │
│   SCD Type 2 для Satellites                                     │
└──────────────────────┬──────────────────────────────────────────┘
                       │ write
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│           APACHE DORIS (FE: 8030/9030, BE: 8040)                │
│                   dwh_detailed                                  │
│                                                                 │
│  HUBs:      hub_user, hub_address, hub_order, hub_product,      │
│             hub_warehouse, hub_pickup_point, hub_shipment       │
│                                                                 │
│  LINKs:     lnk_order_user, lnk_order_product,                  │
│             lnk_user_address, lnk_shipment_order,               │
│             lnk_shipment_warehouse, lnk_shipment_pickup_point   │
│                                                                 │
│  SATs:      sat_user_details, sat_user_address,                 │
│             sat_order_details, sat_product_details,             │
│             sat_warehouse_details, sat_shipment_details,        │
│             sat_order_item_details, sat_pickup_point_details    │
└─────────────────────────────────────────────────────────────────┘

## Структура проекта

hw2/
├── docker-compose.yml
├── .env
├── .gitignore
├── README.md
│
├── init/                          # DDL источников (из ДЗ 1)
│   ├── 00_init_replication.sh
│   ├── 01_user_service_db.sql
│   ├── 02_order_service_db.sql
│   └── 03_logistics_service_db.sql
│
├── replica/                       # Инициализация реплики (из ДЗ 1)
│   └── init_replica.sh
│
├── sql/                           # Когортный анализ (из ДЗ 1)
│   ├── cohort_analysis.sql
│   └── cohort_analysis_view.sql
│
├── debezium/
│   ├── init.sh                    # Регистрация коннекторов
│   └── connectors/
│       ├── user_service_connector.json
│       ├── order_service_connector.json
│       └── logistics_service_connector.json
│
├── ddl_generator/                 # Бонус: генератор DDL
│   ├── generator.py
│   ├── configs/
│   │   ├── user_service.yaml
│   │   ├── order_service.yaml
│   │   └── logistics_service.yaml
│   └── templates/
│       ├── hub.sql.j2
│       ├── link.sql.j2
│       └── satellite.sql.j2
│
├── ddl/
│   └── dwh_detailed.sql           # Сгенерированный DDL для Doris
│
├── doris/
│   ├── fe.conf                    # Конфиг FE (ограничение памяти)
│   ├── be.conf                    # Конфиг BE
│   └── init_ddl.sh                # Применение DDL при старте
│
└── dmp/                           # DMP сервис
    ├── Dockerfile
    ├── requirements.txt
    ├── main.py                    # Запуск воркеров
    ├── loader.py                  # Универсальный DataVaultLoader
    └── configs/                   # YAML-конфиги Hub/Link/Satellite
        ├── hub_user.yaml
        ├── hub_address.yaml
        ├── hub_order.yaml
        ├── hub_product.yaml
        ├── hub_warehouse.yaml
        ├── hub_pickup_point.yaml
        ├── hub_shipment.yaml
        ├── lnk_order_user.yaml
        ├── lnk_order_product.yaml
        ├── lnk_user_address.yaml
        ├── lnk_shipment_order.yaml
        ├── lnk_shipment_warehouse.yaml
        ├── lnk_shipment_pickup_point.yaml
        ├── sat_user_details.yaml
        ├── sat_user_address.yaml
        ├── sat_order_details.yaml
        ├── sat_product_details.yaml
        ├── sat_warehouse_details.yaml
        ├── sat_shipment_details.yaml
        ├── sat_order_item_details.yaml
        └── sat_pickup_point_details.yaml

## Как запустить

### Требования
- Docker >= 20.10
- Docker Compose >= 2.0
- RAM >= 8GB (Apache Doris требует минимум 4GB)

### Запуск

git clone <your-repo-url>
cd hw2

# Запускаем весь стек
docker-compose up -d

# Ждём ~2 минуты пока Doris FE/BE инициализируются
# Следим за статусом
docker-compose ps
```

### Важно после первого запуска

PostgreSQL должен быть настроен на `wal_level = logical` для Debezium:

```bash
docker exec -it pg-master bash -c "
    psql -U postgres -c \"ALTER SYSTEM SET wal_level = logical;\"
    psql -U postgres -c \"ALTER SYSTEM SET max_replication_slots = 10;\"
    psql -U postgres -c \"ALTER SYSTEM SET max_wal_senders = 10;\"
    su -c \"pg_ctl restart -D \$PGDATA -m fast\" postgres
"

# Перерегистрируем коннекторы после рестарта PostgreSQL
docker-compose restart debezium-init
```

### Генератор DDL (Бонус)

Генератор принимает YAML-конфиги источников и создаёт DDL для Data Vault 2.0:

```bash
cd ddl_generator

# Создаём виртуальное окружение
python3 -m venv ~/venv
source ~/venv/bin/activate
pip install pyyaml jinja2

# Генерируем DDL
python generator.py \
  --configs-dir configs \
  --output ../ddl/dwh_detailed.sql \
  --templates-dir templates
```

**Принцип работы:**
1. Читает YAML-конфиг источника (описывает сущности, их BK и атрибуты)
2. Для каждой сущности генерирует Hub + Satellites через Jinja2-шаблоны
3. Для каждой связи генерирует Link + опциональные Satellites
4. Выдаёт готовый SQL-файл для Apache Doris

### Проверка работы

```bash
# Статус всех сервисов
docker-compose ps

# Проверка Debezium коннекторов
curl -s http://localhost:8083/connectors?expand=status | \
  python3 -m json.tool | grep '"state"'

# Проверка топиков Kafka
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 --list

# Проверка данных в Doris
docker exec -it doris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "
USE dwh_detailed;
SELECT 'hub_user' as tbl, COUNT(*) as cnt FROM hub_user
UNION ALL SELECT 'hub_order', COUNT(*) FROM hub_order
UNION ALL SELECT 'hub_product', COUNT(*) FROM hub_product
UNION ALL SELECT 'hub_warehouse', COUNT(*) FROM hub_warehouse;
"

# Логи DMP
docker logs dmp --tail=50
```

### Тест end-to-end

```bash
# Вставляем данные в источник
docker exec -it pg-master psql -U postgres -d user_service_db -c "
INSERT INTO users (user_external_id, email, first_name, last_name, status, is_current, created_at)
VALUES (gen_random_uuid(), 'newuser@test.com', 'New', 'User', 'active', true, NOW());
"

# Через 10 секунд проверяем в Doris
sleep 10
docker exec -it doris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "
USE dwh_detailed;
SELECT * FROM hub_user ORDER BY created_at DESC LIMIT 3;
SELECT * FROM sat_user_details ORDER BY created_at DESC LIMIT 3;
"
```

## Подключение к сервисам

| Сервис | URL / Connection |
|--------|-----------------|
| PostgreSQL Master | `postgresql://postgres:postgres@localhost:5432` |
| PostgreSQL Replica | `postgresql://postgres:postgres@localhost:5433` |
| Apache Doris | `mysql -h localhost -P 9030 -u root` |
| Debezium REST API | `http://localhost:8083` |
| Kafka | `localhost:29092` |
| Kafka UI | `http://localhost:8090` |
| Doris FE HTTP | `http://localhost:8030` |

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

## Источники
- [Data Vault 2.0 Methodology](https://datavaultalliance.com/)
- [Debezium PostgreSQL Connector](https://debezium.io/documentation/reference/connectors/postgresql.html)
- [Apache Doris Docker](https://doris.apache.org/docs/install/cluster-deployment/run-docker-cluster)
- [kafka-python](https://kafka-python.readthedocs.io/)
```
