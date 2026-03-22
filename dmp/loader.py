import hashlib
import json
import logging
from datetime import datetime
from typing import Any

import pymysql
import yaml

logger = logging.getLogger(__name__)


class DataVaultLoader:

    def __init__(self, config_path: str, doris_conn_params: dict):
        self.config = self._load_config(config_path)
        self.conn_params = doris_conn_params
        self.table_type = self.config["table_type"]   # hub | link | satellite
        self.table_name = self.config["table_name"]
        self.topic = self.config["kafka_topic"]
        self.source_system = self.config["source_system"]

    def _load_config(self, path: str) -> dict:
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def _get_connection(self):
        return pymysql.connect(**self.conn_params)

    def _compute_hash(self, data: dict, fields: list) -> str:
        values = "|".join(str(data.get(f, "")) for f in sorted(fields))
        return hashlib.md5(values.encode()).hexdigest()

    def _now(self) -> str:
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def _load_hub(self, data: dict, conn) -> None:
        bk_field = self.config["business_key_field"]
        bk_value = data.get(bk_field)

        if bk_value is None:
            logger.warning(f"[{self.table_name}] BK is None, skipping")
            return

        sql = f"""
            INSERT INTO {self.table_name}
                ({bk_field}, load_dts, record_source)
            VALUES
                (%s, %s, %s)
        """
        with conn.cursor() as cur:
            cur.execute(sql, (str(bk_value), self._now(), self.source_system))
        conn.commit()
        logger.info(f"[HUB] {self.table_name} <- bk={bk_value}")

    def _load_link(self, data: dict, conn) -> None:
        hub_keys = self.config["hub_keys"]

        values = {k: str(data.get(v, "")) for k, v in hub_keys.items()}
        if any(v == "" for v in values.values()):
            logger.warning(f"[{self.table_name}] Some hub keys are None, skipping")
            return

        cols = list(values.keys()) + ["load_dts", "record_source"]
        vals = list(values.values()) + [self._now(), self.source_system]

        placeholders = ", ".join(["%s"] * len(vals))
        cols_str = ", ".join(cols)

        sql = f"INSERT INTO {self.table_name} ({cols_str}) VALUES ({placeholders})"

        with conn.cursor() as cur:
            cur.execute(sql, vals)
        conn.commit()
        logger.info(f"[LINK] {self.table_name} <- {values}")

    def _cast_value(self, value: any, field_name: str) -> any:
        if value is None:
            return None
        if isinstance(value, dict):
            if "value" in value:
                return value["value"]
            return str(value)
        return value

    def _load_satellite(self, data: dict, conn) -> None:
        parent_bk_field = self.config["parent_bk_field"]
        parent_bk_col = self.config["parent_bk_col"]
        attribute_fields = self.config["attribute_fields"]

        parent_bk_value = data.get(parent_bk_field)
        if parent_bk_value is None:
            logger.warning(f"[{self.table_name}] Parent BK is None, skipping")
            return

        attr_values = {f: self._cast_value(data.get(f), f) for f in attribute_fields}

        hash_diff = self._compute_hash(data, attribute_fields)

        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT hash_diff FROM {self.table_name}
                WHERE {parent_bk_col} = %s AND is_current = 1
                LIMIT 1
                """,
                (str(parent_bk_value),)
            )
            row = cur.fetchone()

        if row and row[0] == hash_diff:
            logger.debug(f"[SAT] {self.table_name} — no changes for bk={parent_bk_value}")
            return

        now = self._now()

        if row:
            with conn.cursor() as cur:
               cur.execute(
                   f"""
                   UPDATE {self.table_name}
                   SET load_end_dts = %s, is_current = 0
                   WHERE {parent_bk_col} = %s AND is_current = 1
                   """,
                   (now, str(parent_bk_value))
                )
            conn.commit()

        all_cols = (
            [parent_bk_col]
            + attribute_fields
            + ["load_dts", "is_current", "hash_diff", "record_source"]
        )
        all_vals = (
            [str(parent_bk_value)]
            + [attr_values[f] for f in attribute_fields]
            + [now, 1, hash_diff, self.source_system]
        )

        placeholders = ", ".join(["%s"] * len(all_vals))
        cols_str = ", ".join(all_cols)

        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {self.table_name} ({cols_str}) VALUES ({placeholders})",
                all_vals
            )
        conn.commit()
        logger.info(f"[SAT] {self.table_name} <- bk={parent_bk_value}")

    def process(self, message: dict) -> None:
        op = message.get("__op", "r")
        if op == "d":
            logger.debug(f"[{self.table_name}] Skip delete op")
            return

        conn = self._get_connection()
        try:
            if self.table_type == "hub":
                self._load_hub(message, conn)
            elif self.table_type == "link":
                self._load_link(message, conn)
            elif self.table_type == "satellite":
                self._load_satellite(message, conn)
            else:
                logger.error(f"Unknown table_type: {self.table_type}")
        except Exception as e:
            logger.error(f"Error processing message for {self.table_name}: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
