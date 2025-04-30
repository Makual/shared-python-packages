import json
import os
from types import MappingProxyType

from . import task_sender

DEFAULT_CONNECTION_CONF = MappingProxyType({
    "host": os.environ.get("RABBITMQ_HOST", "localhost"),
    "port": int(os.environ.get("RABBITMQ_PORT", "5672")),
    "login": os.environ.get("RABBITMQ_USERNAME", "guest"),
    "password": os.environ.get("RABBITMQ_PASSWORD", "guest"),
})
# noinspection PyTypeChecker
DEFAULT_TASK_SENDER_CONF: task_sender.Config = MappingProxyType({
    "consuming_log": bool(json.loads(os.environ.get("DEFAULT_CONF_CONSUMING_LOG", "False").lower()))
})

DEFAULT_LLM = os.environ.get("DEFAULT_LLM", "openai")
