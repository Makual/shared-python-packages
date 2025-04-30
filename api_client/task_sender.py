import asyncio
import json
from types import MappingProxyType
from typing import TypedDict, Any, NotRequired

import aio_pika
import aio_pika.abc as pika_types


class Config(TypedDict):
    consuming_log: NotRequired[bool]


async def add_task_to_queue(
        queue_name: str,
        payload: dict,
        channel: pika_types.AbstractChannel,
        conf: Config = MappingProxyType({})
) -> Any:
    callback_queue = await channel.declare_queue(exclusive=True, auto_delete=True)
    response_queue = asyncio.Queue[pika_types.AbstractIncomingMessage](maxsize=1)
    consumer_categories = await callback_queue.consume(callback=response_queue.put, no_ack=True)
    await channel.default_exchange.publish(
        message=aio_pika.Message(
            body=json.dumps(payload, ensure_ascii=False).encode(),
            reply_to=callback_queue.name,
        ),
        routing_key=queue_name,
    )
    response_msg = await response_queue.get()
    if conf.get("consuming_log"): print("[add_task_to_queue] response:", response_msg.body.decode("utf-8"))
    await callback_queue.cancel(consumer_categories)
    response = json.loads(response_msg.body, parse_int=float)
    if response_msg.type == "ErrorResult":
        raise Exception(response_msg.body.decode("utf-8"))
    return response
