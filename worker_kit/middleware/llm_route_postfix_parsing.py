"""Middleware to transform routing_key postfix responsible for selecting the LLM to use
to a context value with "routing_key" key.
"""

from functools import partial

from ..msg_handler import TaskHandler, TaskHandlerResult

SUPPORTED_LLMS = {"openai", "gigachat"}


async def handle_task(next_handler: TaskHandler, task: dict, context: dict) -> TaskHandlerResult:
    routing_key: str = context["routing_key"]
    last_part = next(reversed(routing_key.split(".")), None)
    llm_variant = "openai"  # default
    if last_part in SUPPORTED_LLMS:
        llm_variant = last_part
        context["routing_key"] = routing_key.removesuffix(f".{llm_variant}")
    context["llm_variant"] = llm_variant
    return await next_handler(task, context)


def wrap_handler(next_handler: TaskHandler) -> TaskHandler:
    return partial(handle_task, next_handler)
