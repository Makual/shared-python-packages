"""Middleware to catch exceptions in downstream handlers.
"""

import sys
import traceback
from functools import partial

from .encoding import TaskEncodingException
from ..msg_handler import TaskHandler, TaskHandlerResult


async def handle_task(next_handler: TaskHandler, task: dict, context: dict) -> TaskHandlerResult:
    result: TaskHandlerResult = {
        "body": None,
        "type": "ErrorResult",
        "content_type": None,
        "content_encoding": None,
    }
    try:
        result = await next_handler(task, context)
    except TaskEncodingException as e:
        result["body"] = {"code": 415, "message": traceback.format_exception(e)[-1]}
        print(
            f"ERROR {result['body']['code']}:\n"
            f"	{result['body']['message']}\n"
            f"	Raw task: {context['task_bytes'].decode('utf-8')}"
        )
    except Exception as e:
        result["body"] = {"code": 500, "message": traceback.format_exception(e)[-1]}
        print(f"Worker error, raw task:\n{context['task_bytes'].decode('utf-8')}", file=sys.stderr)
        traceback.print_exception(e)
    return result


def wrap_handler(next_handler: TaskHandler) -> TaskHandler:
    return partial(handle_task, next_handler)
