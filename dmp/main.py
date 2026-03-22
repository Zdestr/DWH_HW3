import logging
import os
import threading
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaConsumer

from loader import DataVaultLoader

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

DORIS_CONN_PARAMS = {
    "host": os.getenv("DORIS_HOST", "doris-fe"),
    "port": int(os.getenv("DORIS_PORT", 9030)),
    "user": os.getenv("DORIS_USER", "root"),
    "password": os.getenv("DORIS_PASSWORD", ""),
    "database": os.getenv("DORIS_DATABASE", "dwh_detailed"),
    "charset": "utf8mb4",
    "autocommit": False,
}

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def run_consumer(config_path: str) -> None:
    loader = DataVaultLoader(config_path, DORIS_CONN_PARAMS)
    logger.info(f"Starting consumer: topic={loader.topic}, table={loader.table_name}")

    consumer = KafkaConsumer(
        loader.topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=f"dmp_{loader.table_name}",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: __import__("json").loads(m.decode("utf-8")),
    )

    for message in consumer:
        try:
            data = message.value
            if data is None:
                continue
            loader.process(data)
        except Exception as e:
            logger.error(f"Error in consumer {loader.table_name}: {e}")


def main():
    configs_dir = Path("/app/configs")
    config_files = sorted(configs_dir.glob("*.yaml"))

    if not config_files:
        logger.error("No config files found in /app/configs")
        return

    logger.info(f"Found {len(config_files)} configs: {[f.name for f in config_files]}")

    threads = []
    for config_path in config_files:
        t = threading.Thread(
            target=run_consumer,
            args=(str(config_path),),
            daemon=True,
            name=f"consumer_{config_path.stem}"
        )
        t.start()
        threads.append(t)
        logger.info(f"Started thread for {config_path.name}")

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
