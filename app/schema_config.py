from functools import lru_cache

from config_loader import load_key_value_config
from mysql import env


DEFAULT_SCHEMA_CONFIG_PATH = "config/openfoodfacts-schema.conf"


@lru_cache(maxsize=None)
def load_schema_config(path=None):
    config_path = path or env("SCHEMA_CONFIG_PATH", DEFAULT_SCHEMA_CONFIG_PATH)
    entries = load_key_value_config(config_path, "schema config")

    feature_columns = _parse_list(_required(entries, "feature_columns"))

    return {
        "meta_columns": _parse_list(_required(entries, "meta_columns")),
        "model_meta_columns": _parse_list(_required(entries, "model_meta_columns")),
        "feature_columns": feature_columns,
        "numeric_mappings": _parse_mappings(_required(entries, "numeric_mappings")),
        "feature_ranges": {
            feature: _parse_range(_required(entries, f"range.{feature}"))
            for feature in feature_columns
        },
    }


def _required(entries, key):
    if key not in entries:
        raise ValueError(f"Missing schema config key: {key}")
    return entries[key]


def _parse_list(value):
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_mappings(value):
    result = []
    for item in value.split(","):
        if not item.strip():
            continue
        source_name, target_name = item.split(":", 1)
        result.append((source_name.strip(), target_name.strip()))
    return result


def _parse_range(value):
    min_value, max_value = value.split(":", 1)
    return float(min_value.strip()), float(max_value.strip())
