from typing import Any

from pydantic import TypeAdapter

type_adapters: dict[str, TypeAdapter] = {}


def validate(message: Any, message_type: type):
    global type_adapters
    type_name = str(message_type)
    validator = type_adapters.get(type_name)
    if not validator:
        type_adapter = TypeAdapter(message_type)
        type_adapters[type_name] = type_adapter
        validator = type_adapters[type_name]
    validator.validate_python(message)
