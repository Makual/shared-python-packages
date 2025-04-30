"""Middleware for routing to proper task handler using "routing_key" in context, according to
provided routes dict.
"""

from functools import partial

from ..msg_handler import TaskHandler, TaskHandlerResult


class RoutingException(Exception):
    pass


async def handle_task(handler_routes: dict[str, TaskHandler], task: dict, context: dict) -> TaskHandlerResult:
    routing_key = context["routing_key"]
    matched_handler = handler_routes.get(routing_key)
    if not matched_handler: raise RoutingException(f'routing key "{routing_key}" not found in routing map')
    return await matched_handler(task, context)


def wrap_handlers(handler_routes: dict[str, TaskHandler]) -> TaskHandler:
    return partial(handle_task, handler_routes)
