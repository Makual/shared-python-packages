from asyncio import create_task
from typing import TypedDict, Literal

import aio_pika
import aio_pika.abc as pika_types

from . import defaults
from . import message_validation
from . import task_sender

LLM_CONNECTOR_QUEUE_NAME = "llm-connector-tasks"
VECTORIZER_QUEUE_NAME = "vectorizer-tasks"


class Config(task_sender.Config):
    llm: Literal["openai", "gigachat"]


class ChatMessage(TypedDict):
    role: Literal["system", "user", "assistant"]
    content: str


class Client:
    channel: pika_types.AbstractChannel
    conf: Config

    def __init__(self, channel: pika_types.AbstractChannel, conf: Config):
        self.channel = channel
        self.conf = conf

    async def get_embedding(self, input: list[str], is_query: bool = None) -> list[list[float]]:
        message_validation.validate(input, list[str])
        return await task_sender.add_task_to_queue(
            VECTORIZER_QUEUE_NAME, {"input": input, "is_query": is_query}, self.channel, self.conf,
        )

    async def create_chat_completion(
            self,
            messages: list[ChatMessage],
            model: str = None,
            temperature: float = None,
            top_p: float = None,
            max_tokens: int = None,
            repetition_penalty: float = None,
    ) -> str:
        message_validation.validate(messages, list[ChatMessage])
        task = {
            "messages": messages,
            "model": model,
            "temperature": temperature,
            "top_p": top_p,
            "max_tokens": max_tokens,
            "repetition_penalty": repetition_penalty,
        }
        return await task_sender.add_task_to_queue(
            f"{LLM_CONNECTOR_QUEUE_NAME}.{self.conf['llm']}", task, self.channel, self.conf,
        )


async def get_embedding(input: list[str], is_query: bool = None) -> list[list[float]]:
    return await (await get_default_client()).get_embedding(input, is_query)


async def create_chat_completion(
        messages: list[ChatMessage],
        model: str = None,
        temperature: float = None,
        top_p: float = None,
        max_tokens: int = None,
        repetition_penalty: float = None,
) -> str:
    return await (await get_default_client()).create_chat_completion(
        messages, model, temperature, top_p, max_tokens, repetition_penalty
    )


async def get_default_client() -> Client:
    async def create_client():
        rabbit_conn = await aio_pika.connect_robust(**defaults.DEFAULT_CONNECTION_CONF)
        channel = await rabbit_conn.channel()
        return Client(channel, {"llm": defaults.DEFAULT_LLM, **defaults.DEFAULT_TASK_SENDER_CONF})

    if not '_default_client' in globals():
        _default_client = create_task(create_client())
    return await _default_client
