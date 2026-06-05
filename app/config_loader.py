def load_key_value_config(path, config_name):
    result = {}
    with open(path, encoding="utf-8") as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("=", 1)
            if len(parts) != 2:
                raise ValueError(f"Invalid {config_name} line in '{path}': {line}")
            result[parts[0].strip()] = parts[1].strip()
    return result

