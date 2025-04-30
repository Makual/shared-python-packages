"""Middleware for converting binary data of task bytes to and from dictionary.
"""

import gzip
import json
from functools import partial

from ..msg_handler import TaskHandler, TaskHandlerResult


class TaskEncodingException(Exception):
    pass


async def encode_task(next_handler: TaskHandler, _task, context: dict) -> TaskHandlerResult:
    if context["content_type"] != "application/json":
        raise TaskEncodingException(
            f"unsupported content type: {context['content_type']}, see documentation for proper type",
        )
    compression = context["content_encoding"]
    if compression:
        if compression == "gzip":
            context["task_bytes"] = gzip.decompress(context["task_bytes"])
        else:
            raise TaskEncodingException(f"unsupported compression: {compression}, only gzip is supported")
    try:
        task = json.loads(context["task_bytes"], parse_int=float)
    except Exception as e:
        raise TaskEncodingException(f"can't parse JSON of the task: {repr(e)}")
    return await next_handler(task, context)


async def decode_result(next_handler: TaskHandler, task, context: dict) -> TaskHandlerResult:
    result = await next_handler(task, context)
    result["body"] = json.dumps(result["body"], ensure_ascii=False).encode()
    result["content_type"] = "application/json"
    compression = context["content_encoding"]
    if compression:
        result["body"] = gzip.compress(result["body"])
        result["content_encoding"] = "gzip"
    return result


def wrap_handler_with_encoder(next_handler: TaskHandler) -> TaskHandler:
    return partial(encode_task, next_handler)


def wrap_handler_with_decoder(next_handler: TaskHandler) -> TaskHandler:
    return partial(decode_result, next_handler)
