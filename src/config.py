"""Configuration loader supporting config.yaml, .env files, and ${VAR:-default} interpolation."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}")


def load_dotenv(env_path: str | Path = ".env") -> None:
    """Load key=value pairs from a .env file into os.environ without overriding existing vars."""
    path = Path(env_path)
    if not path.is_file():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


def _interpolate_env(obj: Any) -> Any:
    """Recursively expand ${VAR} and ${VAR:-default} placeholders using os.environ."""
    if isinstance(obj, dict):
        return {k: _interpolate_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_interpolate_env(item) for item in obj]
    if isinstance(obj, str):
        def _replace(match: re.Match[str]) -> str:
            var_name = match.group(1)
            default = match.group(2) if match.group(2) is not None else ""
            return os.environ.get(var_name, default)

        return ENV_PATTERN.sub(_replace, obj)
    return obj


def _parse_scalar(raw: str) -> Any:
    """Parse a YAML scalar value (bool, int, float, null, or string)."""
    value = raw.strip()
    if not value or value in ("null", "Null", "NULL", "~"):
        return None
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
        return value[1:-1]
    lower = value.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    try:
        if "." not in value and "e" not in lower:
            return int(value)
        return float(value)
    except ValueError:
        return value


def _strip_inline_comment(line: str) -> str:
    """Remove trailing YAML comments outside quotes."""
    in_single = False
    in_double = False
    for idx, ch in enumerate(line):
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "#" and not in_single and not in_double:
            if idx == 0 or line[idx - 1].isspace():
                return line[:idx].rstrip()
    return line.rstrip()


def _parse_yaml_fallback(text: str) -> Dict[str, Any]:
    """Minimal indentation-based YAML parser for environments without PyYAML installed.

    Supports nested mappings, sequences of scalars, and sequences of mappings.
    """
    tokens: List[Tuple[int, str]] = []
    for raw_line in text.splitlines():
        cleaned = _strip_inline_comment(raw_line)
        if not cleaned.strip():
            continue
        indent = len(cleaned) - len(cleaned.lstrip(" "))
        tokens.append((indent, cleaned.lstrip(" ")))

    def _parse_block(index: int, min_indent: int) -> Tuple[Any, int]:
        if index >= len(tokens):
            return {}, index

        first_indent, first_text = tokens[index]
        if first_text.startswith("- ") or first_text == "-":
            items: List[Any] = []
            while index < len(tokens):
                indent, text_line = tokens[index]
                if indent < min_indent or not (text_line.startswith("- ") or text_line == "-"):
                    break
                if indent > min_indent:
                    break
                rest = text_line[2:].strip() if text_line.startswith("- ") else ""
                index += 1
                if not rest:
                    child, index = _parse_block(index, indent + 1)
                    items.append(child)
                elif ":" in rest and not (rest.startswith('"') or rest.startswith("'")):
                    # Sequence item that starts a mapping (e.g., "- name: new_york")
                    k, v = rest.split(":", 1)
                    item_dict: Dict[str, Any] = {}
                    v_str = v.strip()
                    if v_str:
                        item_dict[k.strip()] = _parse_scalar(v_str)
                    else:
                        child, index = _parse_block(index, indent + 2)
                        item_dict[k.strip()] = child
                    # Parse any additional keys belonging to this list-item mapping
                    while index < len(tokens) and tokens[index][0] > indent:
                        sub_indent, sub_line = tokens[index]
                        if sub_line.startswith("- "):
                            break
                        sk, sv = sub_line.split(":", 1)
                        index += 1
                        sv_str = sv.strip()
                        if sv_str:
                            item_dict[sk.strip()] = _parse_scalar(sv_str)
                        else:
                            child, index = _parse_block(index, sub_indent + 1)
                            item_dict[sk.strip()] = child
                    items.append(item_dict)
                else:
                    items.append(_parse_scalar(rest))
            return items, index

        mapping: Dict[str, Any] = {}
        while index < len(tokens):
            indent, text_line = tokens[index]
            if indent < min_indent:
                break
            if text_line.startswith("- "):
                break
            if ":" not in text_line:
                index += 1
                continue
            key, val = text_line.split(":", 1)
            key = key.strip()
            val_str = val.strip()
            index += 1
            if val_str:
                mapping[key] = _parse_scalar(val_str)
            else:
                if index < len(tokens) and tokens[index][0] > indent:
                    child, index = _parse_block(index, tokens[index][0])
                    mapping[key] = child
                else:
                    mapping[key] = {}
        return mapping, index

    parsed, _ = _parse_block(0, 0)
    if not isinstance(parsed, dict):
        raise ValueError("Top-level configuration must be a YAML mapping.")
    return parsed


def load_config(
    config_path: str | Path = "config.yaml",
    env_path: Optional[str | Path] = ".env",
) -> Dict[str, Any]:
    """Load YAML configuration and interpolate environment variables."""
    if env_path is not None:
        load_dotenv(env_path)

    path = Path(config_path)
    if not path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    raw_text = path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore

        data = yaml.safe_load(raw_text) or {}
    except ImportError:
        data = _parse_yaml_fallback(raw_text)

    if not isinstance(data, dict):
        raise ValueError(f"Invalid configuration structure in {path}")

    return _interpolate_env(data)
