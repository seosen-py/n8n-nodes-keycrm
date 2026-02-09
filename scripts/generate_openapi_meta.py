#!/usr/bin/env python3
"""Generate UI metadata for KeyCRM n8n node from OpenAPI."""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any

import yaml


ROOT_DIR = Path(__file__).resolve().parents[1]
OPENAPI_YML_PATH = ROOT_DIR / "open-api.yml"
OPENAPI_JSON_PATH = ROOT_DIR / "open-api.json"
OUTPUT_PATH = ROOT_DIR / "nodes" / "Keycrm" / "openapi-data.json"
HTTP_METHODS = {"get", "post", "put", "patch", "delete"}


def read_openapi_spec() -> dict[str, Any]:
    if OPENAPI_YML_PATH.exists():
        return yaml.safe_load(OPENAPI_YML_PATH.read_text(encoding="utf-8"))
    if OPENAPI_JSON_PATH.exists():
        return json.loads(OPENAPI_JSON_PATH.read_text(encoding="utf-8"))
    raise FileNotFoundError(
        f"Missing OpenAPI file. Expected one of: {OPENAPI_YML_PATH.name}, {OPENAPI_JSON_PATH.name}"
    )


class OpenApiResolver:
    def __init__(self, spec: dict[str, Any]) -> None:
        self.spec = spec

    def resolve_ref(self, ref: str) -> Any:
        if not ref.startswith("#/"):
            raise ValueError(f"Unsupported external $ref: {ref}")
        node: Any = self.spec
        for token in ref[2:].split("/"):
            node = node[token]
        return copy.deepcopy(node)

    def resolve_value(self, value: Any) -> Any:
        if isinstance(value, dict):
            if "$ref" in value:
                resolved = self.resolve_ref(value["$ref"])
                overlays = {k: self.resolve_value(v) for k, v in value.items() if k != "$ref"}
                if isinstance(resolved, dict):
                    resolved.update(overlays)
                    value = resolved
                else:
                    value = overlays or resolved
            else:
                value = {k: self.resolve_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            value = [self.resolve_value(item) for item in value]
        return value

    def resolve_schema(self, schema: dict[str, Any] | None) -> dict[str, Any]:
        if not schema:
            return {}
        resolved = self.resolve_value(schema)
        if not isinstance(resolved, dict):
            return {}

        if "allOf" in resolved and isinstance(resolved["allOf"], list):
            merged = self.merge_all_of(resolved["allOf"])
            for key, val in resolved.items():
                if key == "allOf":
                    continue
                if key == "required" and isinstance(val, list):
                    merged["required"] = sorted(set(merged.get("required", []) + val))
                elif key == "properties" and isinstance(val, dict):
                    properties = merged.setdefault("properties", {})
                    properties.update(val)
                else:
                    merged[key] = val
            resolved = merged

        if isinstance(resolved.get("properties"), dict):
            resolved["properties"] = {
                name: self.resolve_schema(prop_schema)
                for name, prop_schema in resolved["properties"].items()
            }

        if isinstance(resolved.get("items"), dict):
            resolved["items"] = self.resolve_schema(resolved["items"])

        return resolved

    def merge_all_of(self, items: list[Any]) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        required: list[str] = []
        properties: dict[str, Any] = {}

        for item in items:
            schema = self.resolve_schema(item if isinstance(item, dict) else {})
            if not schema:
                continue
            for key, val in schema.items():
                if key == "required" and isinstance(val, list):
                    required.extend([str(entry) for entry in val])
                elif key == "properties" and isinstance(val, dict):
                    properties.update(val)
                else:
                    merged[key] = val

        if required:
            merged["required"] = sorted(set(required))
        if properties:
            merged["properties"] = properties
        return merged


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def humanize_identifier(value: str) -> str:
    text = value.replace(".", " ").replace("_", " ").replace("-", " ")
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
    text = normalize_space(text)
    if not text:
        return value
    return text[0].upper() + text[1:].lower()


def operation_label_from_id(operation_id: str) -> str:
    if not operation_id:
        return "Operation"
    sentence = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", operation_id)
    sentence = normalize_space(sentence)
    return sentence[0].upper() + sentence[1:].lower()


def slugify(value: str) -> str:
    lowered = value.lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", lowered).strip("_")
    return normalized or "general"


def infer_schema_type(schema: dict[str, Any], fallback: Any = None) -> str:
    schema_type = schema.get("type")
    if isinstance(schema_type, str):
        return schema_type
    if isinstance(fallback, bool):
        return "boolean"
    if isinstance(fallback, int) and not isinstance(fallback, bool):
        return "integer"
    if isinstance(fallback, float):
        return "number"
    if isinstance(fallback, list):
        return "array"
    if isinstance(fallback, dict):
        return "object"
    return "string"


def parse_include_options(parameter: dict[str, Any]) -> list[str]:
    options: list[str] = []
    description = parameter.get("description") or ""

    for token in re.findall(r"`([^`]+)`", description):
        cleaned = normalize_space(token)
        if cleaned:
            options.append(cleaned)

    for token in re.findall(r"<strong>([^<]+)</strong>", description, flags=re.IGNORECASE):
        for part in token.split(","):
            cleaned = normalize_space(part)
            if cleaned:
                options.append(cleaned)

    examples = parameter.get("examples")
    if isinstance(examples, dict):
        for example in examples.values():
            if not isinstance(example, dict):
                continue
            value = example.get("value")
            if isinstance(value, str):
                for part in value.split(","):
                    cleaned = normalize_space(part)
                    if cleaned:
                        options.append(cleaned)

    deduplicated: list[str] = []
    seen: set[str] = set()
    for option in options:
        if option not in seen:
            seen.add(option)
            deduplicated.append(option)
    return deduplicated


def parse_sort_options(parameter: dict[str, Any]) -> list[dict[str, str]]:
    options: list[dict[str, str]] = []
    examples = parameter.get("examples")
    if isinstance(examples, dict):
        for key, example in examples.items():
            if not isinstance(example, dict):
                continue
            raw_value = example.get("value")
            if raw_value is None:
                continue
            value = str(raw_value)
            name = normalize_space(example.get("summary") or humanize_identifier(value) or key)
            options.append({"name": name, "value": value})

    schema = parameter.get("schema")
    if isinstance(schema, dict):
        enum_values = schema.get("enum")
        if isinstance(enum_values, list):
            for enum_value in enum_values:
                value = str(enum_value)
                if not any(option["value"] == value for option in options):
                    options.append({"name": humanize_identifier(value), "value": value})

    return options


def parse_filter_fields(parameter: dict[str, Any], resolver: OpenApiResolver) -> list[dict[str, Any]]:
    fields: dict[str, dict[str, Any]] = {}

    examples = parameter.get("examples")
    if isinstance(examples, dict):
        for example in examples.values():
            if not isinstance(example, dict):
                continue
            value = example.get("value")
            if not isinstance(value, dict):
                continue
            summary = normalize_space(example.get("summary") or "")
            for key, field_example in value.items():
                if key not in fields:
                    if key.endswith("_between"):
                        field_type = "betweenDateTime"
                    elif isinstance(field_example, bool):
                        field_type = "boolean"
                    elif isinstance(field_example, int) and not isinstance(field_example, bool):
                        field_type = "integer"
                    elif isinstance(field_example, float):
                        field_type = "number"
                    else:
                        field_type = "string"
                    fields[key] = {
                        "name": key,
                        "label": humanize_identifier(key),
                        "description": summary,
                        "fieldType": field_type,
                        "example": field_example,
                    }
                elif summary and not fields[key].get("description"):
                    fields[key]["description"] = summary

    if not fields:
        schema = resolver.resolve_schema(parameter.get("schema"))
        properties = schema.get("properties")
        if isinstance(properties, dict):
            for key, raw_schema in properties.items():
                if not isinstance(raw_schema, dict):
                    continue
                schema_type = infer_schema_type(raw_schema, raw_schema.get("example"))
                fields[key] = {
                    "name": key,
                    "label": humanize_identifier(key),
                    "description": normalize_space(raw_schema.get("description") or ""),
                    "fieldType": "betweenDateTime" if key.endswith("_between") else schema_type,
                    "example": raw_schema.get("example"),
                }

    return [fields[key] for key in sorted(fields)]


def normalize_required_names(required: Any) -> list[str]:
    if isinstance(required, bool):
        return []
    if isinstance(required, list):
        return [str(name) for name in required]
    return []


def parse_body_field(
    resolver: OpenApiResolver,
    name: str,
    schema: dict[str, Any],
    required: bool,
    parent_path: str,
) -> dict[str, Any]:
    resolved = resolver.resolve_schema(schema)
    api_path = f"{parent_path}.{name}" if parent_path else name
    schema_type = infer_schema_type(resolved, resolved.get("example"))
    field: dict[str, Any] = {
        "kind": "primitive",
        "apiKey": name,
        "apiPath": api_path,
        "label": humanize_identifier(name),
        "description": normalize_space(resolved.get("description") or ""),
        "required": required,
        "nullable": bool(resolved.get("nullable", False)),
    }

    if schema_type == "object" or isinstance(resolved.get("properties"), dict):
        required_names = set(normalize_required_names(resolved.get("required")))
        children: list[dict[str, Any]] = []
        properties = resolved.get("properties") or {}
        for child_name, child_schema in properties.items():
            if not isinstance(child_schema, dict):
                continue
            children.append(
                parse_body_field(
                    resolver=resolver,
                    name=child_name,
                    schema=child_schema,
                    required=child_name in required_names,
                    parent_path=api_path,
                )
            )
        field["kind"] = "object"
        field["children"] = children
        return field

    if schema_type == "array":
        item_schema = resolved.get("items")
        if not isinstance(item_schema, dict):
            item_schema = {}
        item_field = parse_body_field(
            resolver=resolver,
            name="value",
            schema=item_schema,
            required=True,
            parent_path=f"{api_path}[]",
        )
        field["kind"] = "array"
        field["itemField"] = item_field
        return field

    field["schemaType"] = schema_type
    field["format"] = resolved.get("format")
    field["enumValues"] = [str(value) for value in resolved.get("enum", [])] if isinstance(resolved.get("enum"), list) else []
    field["example"] = resolved.get("example")
    field["default"] = resolved.get("default")
    return field


def build_body_ui(operation: dict[str, Any], resolver: OpenApiResolver) -> dict[str, Any] | None:
    request_body = resolver.resolve_value(operation.get("requestBody"))
    if not isinstance(request_body, dict):
        return None

    content = request_body.get("content")
    if not isinstance(content, dict) or not content:
        return None

    preferred_content_types = ["application/json", "multipart/form-data", "application/x-www-form-urlencoded"]
    content_type = next((ct for ct in preferred_content_types if ct in content), next(iter(content.keys())))
    media_type = content.get(content_type)
    if not isinstance(media_type, dict):
        return None

    schema = resolver.resolve_schema(media_type.get("schema"))
    if not schema:
        return None

    required_names = set(normalize_required_names(schema.get("required")))
    required_fields: list[dict[str, Any]] = []
    optional_fields: list[dict[str, Any]] = []

    if isinstance(schema.get("properties"), dict):
        for field_name, field_schema in schema["properties"].items():
            if not isinstance(field_schema, dict):
                continue
            parsed = parse_body_field(
                resolver=resolver,
                name=field_name,
                schema=field_schema,
                required=field_name in required_names,
                parent_path="",
            )
            if field_name in required_names:
                required_fields.append(parsed)
            else:
                optional_fields.append(parsed)
    else:
        parsed = parse_body_field(
            resolver=resolver,
            name="value",
            schema=schema,
            required=True,
            parent_path="",
        )
        required_fields.append(parsed)

    binary_property = None
    if content_type == "multipart/form-data":
        for field in required_fields + optional_fields:
            if field.get("kind") != "primitive":
                continue
            if field.get("format") == "binary":
                binary_property = field.get("apiKey")
                break

    return {
        "contentType": content_type,
        "binaryProperty": binary_property,
        "requiredFields": required_fields,
        "optionalFields": optional_fields,
    }


def build_query_ui(parameters: list[dict[str, Any]], resolver: OpenApiResolver) -> dict[str, Any]:
    simple_fields: list[dict[str, Any]] = []
    include_field: dict[str, Any] | None = None
    sort_field: dict[str, Any] | None = None
    filter_fields: list[dict[str, Any]] = []

    for parameter in parameters:
        if parameter.get("in") != "query":
            continue
        name = parameter.get("name")
        if not isinstance(name, str) or not name:
            continue

        if name == "include":
            options = parse_include_options(parameter)
            include_field = {
                "name": "include",
                "label": "Include",
                "description": normalize_space(parameter.get("description") or ""),
                "options": [{"name": humanize_identifier(value), "value": value} for value in options],
            }
            continue

        if name == "sort":
            sort_field = {
                "name": "sort",
                "label": "Sort",
                "description": normalize_space(parameter.get("description") or ""),
                "options": parse_sort_options(parameter),
            }
            continue

        if name == "filter" and parameter.get("style") == "deepObject":
            filter_fields = parse_filter_fields(parameter, resolver)
            continue

        schema = resolver.resolve_schema(parameter.get("schema"))
        schema_type = infer_schema_type(schema, parameter.get("example"))
        simple_fields.append(
            {
                "name": name,
                "apiPath": name,
                "label": humanize_identifier(name),
                "description": normalize_space(parameter.get("description") or ""),
                "required": bool(parameter.get("required", False)),
                "schemaType": schema_type,
                "format": schema.get("format"),
                "enumValues": [str(value) for value in schema.get("enum", [])]
                if isinstance(schema.get("enum"), list)
                else [],
                "minimum": schema.get("minimum"),
                "maximum": schema.get("maximum"),
                "default": schema.get("default"),
                "example": schema.get("example") or parameter.get("example"),
            }
        )

    simple_fields.sort(key=lambda item: item["label"])

    return {
        "simple": simple_fields,
        "include": include_field,
        "sort": sort_field,
        "filters": filter_fields,
    }


def resolve_parameters(
    resolver: OpenApiResolver,
    path_item: dict[str, Any],
    operation: dict[str, Any],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for source in (path_item.get("parameters"), operation.get("parameters")):
        if not isinstance(source, list):
            continue
        for parameter in source:
            resolved = resolver.resolve_value(parameter)
            if isinstance(resolved, dict):
                merged.append(resolved)
    return merged


def build_operation_meta(
    resolver: OpenApiResolver,
    path: str,
    method: str,
    path_item: dict[str, Any],
    operation: dict[str, Any],
) -> dict[str, Any]:
    operation_id = operation.get("operationId") or f"{method.lower()}_{slugify(path)}"
    parameters = resolve_parameters(resolver, path_item, operation)

    path_ui: list[dict[str, Any]] = []
    for parameter in parameters:
        if parameter.get("in") != "path":
            continue
        name = parameter.get("name")
        if not isinstance(name, str) or not name:
            continue
        path_ui.append(
            {
                "name": name,
                "apiPath": name,
                "label": humanize_identifier(name),
                "description": normalize_space(parameter.get("description") or ""),
                "required": bool(parameter.get("required", False)),
                "example": parameter.get("example"),
            }
        )

    query_ui = build_query_ui(parameters, resolver)
    body_ui = build_body_ui(operation, resolver)

    tag = operation.get("tags", ["General"])
    tag_value = tag[0] if isinstance(tag, list) and tag else "General"

    return {
        "resourceValue": slugify(str(tag_value)),
        "resourceLabel": str(tag_value),
        "operationValue": operation_id,
        "operationId": operation_id,
        "operationLabel": operation_label_from_id(operation_id),
        "method": method.upper(),
        "path": path,
        "summary": normalize_space(operation.get("summary") or ""),
        "description": normalize_space(operation.get("description") or operation.get("summary") or ""),
        "pathUi": sorted(path_ui, key=lambda item: item["label"]),
        "queryUi": query_ui,
        "bodyUi": body_ui,
    }


def generate_metadata(spec: dict[str, Any]) -> dict[str, Any]:
    resolver = OpenApiResolver(spec)
    resources: dict[str, dict[str, Any]] = {}
    operation_count = 0

    for path, path_item in spec.get("paths", {}).items():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method.lower() not in HTTP_METHODS:
                continue
            if not isinstance(operation, dict):
                continue

            operation_meta = build_operation_meta(
                resolver=resolver,
                path=path,
                method=method,
                path_item=path_item,
                operation=operation,
            )

            resource_key = operation_meta["resourceValue"]
            resource_entry = resources.setdefault(
                resource_key,
                {
                    "resourceValue": operation_meta["resourceValue"],
                    "resourceLabel": operation_meta["resourceLabel"],
                    "operations": [],
                },
            )
            resource_entry["operations"].append(
                {
                    key: value
                    for key, value in operation_meta.items()
                    if key not in {"resourceValue", "resourceLabel"}
                }
            )
            operation_count += 1

    sorted_resources = sorted(resources.values(), key=lambda entry: entry["resourceLabel"])
    for resource in sorted_resources:
        resource["operations"] = sorted(resource["operations"], key=lambda entry: entry["operationLabel"])

    return {
        "resources": sorted_resources,
        "operationCount": operation_count,
    }


def main() -> None:
    spec = read_openapi_spec()
    metadata = generate_metadata(spec)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Generated {metadata['operationCount']} operations into {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
