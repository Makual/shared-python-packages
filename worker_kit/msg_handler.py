"""Message handler for aio_pika that converts aio_pika messages into task and context
dictionaries, passes them to the provided handler, and publishes the result to the
"reply_to" queue.
"""

from functools import partial
from typing import Callable, Awaitable, TypedDict, Any, Optional

import aio_pika
import aio_pika.abc as pika_types

DEFAULT_CONTENT_TYPE = "application/json"


class TaskHandlerResult(TypedDict):
    body: Any
    type: str
    content_type: Optional[str]
    content_encoding: Optional[str]


MessageHandler = Callable[[pika_types.AbstractIncomingMessage], Awaitable[Any]]
TaskHandler = Callable[[dict, dict], Awaitable[TaskHandlerResult]]


async def handle_message(task_handler: TaskHandler, message: pika_types.AbstractIncomingMessage):
    await message.ack()
    context = {
        "task_bytes": message.body,
        "content_type": message.content_type or DEFAULT_CONTENT_TYPE,
        "content_encoding": message.content_encoding,
        "routing_key": message.routing_key,
    }
    result = await task_handler({}, context)
    if message.reply_to:
        reply_message = aio_pika.Message(
            body=result["body"],
            type=result["type"],
            content_type=result["content_type"] or DEFAULT_CONTENT_TYPE,
            content_encoding=result["content_encoding"],
            correlation_id=message.correlation_id,
        )
        await message.channel.basic_publish(
            body=reply_message.body,
            exchange=message.exchange,
            routing_key=message.reply_to,
            properties=reply_message.properties,
        )


def wrap_handler(task_handler: TaskHandler) -> MessageHandler:
    return partial(handle_message, task_handler)
