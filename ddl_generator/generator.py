#!/usr/bin/env python3

import yaml
import os
import argparse
from pathlib import Path
from jinja2 import Environment, FileSystemLoader


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def render_template(env: Environment, template_name: str, context: dict) -> str:
    template = env.get_template(template_name)
    return template.render(**context)


def generate_ddl(config_path: str, output_path: str, templates_dir: str) -> None:
    config = load_config(config_path)
    source_system = config["source_system"]

    env = Environment(
        loader=FileSystemLoader(templates_dir),
        trim_blocks=True,
        lstrip_blocks=True
    )

    sql_parts = []
    sql_parts.append(f"-- ============================================================")
    sql_parts.append(f"-- DDL сгенерирован автоматически из: {config_path}")
    sql_parts.append(f"-- Source system: {source_system}")
    sql_parts.append(f"-- ============================================================")
    sql_parts.append("")

    # Генерируем HUBs
    for entity in config.get("entities", []):
        hub = entity["hub"]
        sql_parts.append(render_template(env, "hub.sql.j2", {
            "hub_name": hub["name"],
            "business_key_field": hub["business_key"]["field"],
            "business_key_type": hub["business_key"]["type"],
            "source_system": source_system
        }))

    # Генерируем LINKs
    for link in config.get("links", []):
        sql_parts.append(render_template(env, "link.sql.j2", {
            "link_name": link["name"],
            "hubs": link["hubs"],
            "source_system": source_system
        }))

    # Генерируем SATs для entities
    for entity in config.get("entities", []):
        for sat in entity.get("satellites", []):
            hub_name = entity["hub"]["name"]
            sql_parts.append(render_template(env, "satellite.sql.j2", {
                "sat_name": sat["name"],
                "parent_name": hub_name,
                "parent_type": "hub",
                "fields": sat["fields"],
                "source_system": source_system
            }))

    # Генерируем SATs для links
    for link in config.get("links", []):
        for sat in link.get("satellites", []):
            sql_parts.append(render_template(env, "satellite.sql.j2", {
                "sat_name": sat["name"],
                "parent_name": link["name"],
                "parent_type": "link",
                "fields": sat["fields"],
                "source_system": source_system
            }))

    sql_output = "\n".join(sql_parts)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.write(sql_output)

    print(f"DDL сгенерирован: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Data Vault 2.0 DDL Generator")
    parser.add_argument(
        "--configs-dir",
        default="configs",
        help="Директория с YAML-конфигами источников"
    )
    parser.add_argument(
        "--output",
        default="../ddl/dwh_detailed.sql",
        help="Путь к выходному SQL файлу"
    )
    parser.add_argument(
        "--templates-dir",
        default="templates",
        help="Директория с Jinja2-шаблонами"
    )
    args = parser.parse_args()

    configs_dir = Path(args.configs_dir)
    all_sql_parts = []

    all_sql_parts.append("-- ============================================================")
    all_sql_parts.append("-- DWH Detailed Layer - Data Vault 2.0")
    all_sql_parts.append("-- Apache Doris")
    all_sql_parts.append("-- Сгенерировано автоматически DDL Generator")
    all_sql_parts.append("-- ============================================================")
    all_sql_parts.append("")
    all_sql_parts.append("CREATE DATABASE IF NOT EXISTS dwh_detailed;")
    all_sql_parts.append("USE dwh_detailed;")
    all_sql_parts.append("")

    env = Environment(
        loader=FileSystemLoader(args.templates_dir),
        trim_blocks=True,
        lstrip_blocks=True
    )

    for config_file in sorted(configs_dir.glob("*.yaml")):
        config = load_config(str(config_file))
        source_system = config["source_system"]

        all_sql_parts.append(f"-- ============================================================")
        all_sql_parts.append(f"-- Source: {source_system}")
        all_sql_parts.append(f"-- ============================================================")
        all_sql_parts.append("")

        for entity in config.get("entities", []):
            hub = entity["hub"]
            all_sql_parts.append(render_template(env, "hub.sql.j2", {
                "hub_name": hub["name"],
                "business_key_field": hub["business_key"]["field"],
                "business_key_type": hub["business_key"]["type"],
                "source_system": source_system
            }))

        for link in config.get("links", []):
            all_sql_parts.append(render_template(env, "link.sql.j2", {
                "link_name": link["name"],
                "hubs": link["hubs"],
                "source_system": source_system
            }))

        for entity in config.get("entities", []):
            for sat in entity.get("satellites", []):
                hub_name = entity["hub"]["name"]
                all_sql_parts.append(render_template(env, "satellite.sql.j2", {
                    "sat_name": sat["name"],
                    "parent_name": hub_name,
                    "parent_type": "hub",
                    "fields": sat["fields"],
                    "source_system": source_system
                }))

        for link in config.get("links", []):
            for sat in link.get("satellites", []):
                all_sql_parts.append(render_template(env, "satellite.sql.j2", {
                    "sat_name": sat["name"],
                    "parent_name": link["name"],
                    "parent_type": "link",
                    "fields": sat["fields"],
                    "source_system": source_system
                }))

    output_path = Path(args.output)
    os.makedirs(output_path.parent, exist_ok=True)

    with open(output_path, "w") as f:
        f.write("\n".join(all_sql_parts))

    print(f"✅ Полный DDL сгенерирован: {output_path}")


if __name__ == "__main__":
    main()
