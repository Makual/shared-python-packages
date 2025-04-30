"""Middleware for fetching and attaching config dict to context. This implementation fetches
config from "config" DB table.
"""

from functools import partial

import asyncpg

from ..msg_handler import TaskHandler, TaskHandlerResult


async def handle_task(
        name: str,
        db_pool: asyncpg.Pool,
        next_handler: TaskHandler,
        task: dict,
        context: dict,
) -> TaskHandlerResult:
    config = {}
    db_records: list[asyncpg.Record] = await db_pool.fetch("SELECT key, value, default_value FROM config")
    for record in db_records:
        value = record["default_value"] if record["value"] is None else record["value"]
        config = _add_branch(config, record["key"].split("."), value)
    context["config"] = config.get(name, {})
    return await next_handler(task, context)


def wrap_handler(name: str, db_pool: asyncpg.Pool, next_handler: TaskHandler) -> TaskHandler:
    return partial(handle_task, name, db_pool, next_handler)


def _add_branch(tree: dict, vector: list, value):
    # recursive config dict construction function
    key = vector[0]
    tree[key] = value if len(vector) == 1 else _add_branch(
        tree[key] if key in tree else {},
        vector[1:],
        value,
    )
    return tree
