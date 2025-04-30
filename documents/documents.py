import operator
from operator import itemgetter
from typing import TypedDict, Optional, Callable, Any

import asyncpg
import pydantic
from api_client import utility_client

import preprocessing

SQL_INSERT_DOCS = '''
WITH
	inserted_ids AS (
		INSERT INTO docs (title, text, action_url, source_url) VALUES ($1, $2, $3, $4)
			RETURNING id
	),
	doc_ids_with_cats AS (
		SELECT * FROM (SELECT (row_number() OVER ()), * FROM inserted_ids)
			JOIN (SELECT (row_number() OVER ()), * FROM (VALUES ($5::int[])) AS _ (category_ids))
			USING (row_number)
	),
	_ AS (
		INSERT INTO doc_categories (doc_id, category_id)
			SELECT id doc_id, unnest(category_ids) category_id FROM doc_ids_with_cats
	)
SELECT * FROM inserted_ids
'''
SQL_GET_DOCS = '''
SELECT
	id, title, text, action_url, source_url,
		ARRAY(SELECT category_id FROM doc_categories WHERE doc_id = id) categoiries
	FROM docs WHERE id = ANY($1)
'''
SQL_GET_DOCS_BY_CAT = '''
WITH required AS (
	SELECT id FROM docs LEFT JOIN doc_categories on doc_id = id
	GROUP BY id HAVING array_agg(category_id) @> $1::int[]
)
SELECT id FROM required LEFT JOIN doc_categories on doc_id = id
WHERE cardinality($2::int[]) = 0 OR category_id = ANY ($2::int[])
GROUP BY id
'''
SQL_UPDATE_DOCS = '''
WITH
	_1 AS (DELETE FROM doc_categories WHERE doc_id = $1 AND NOT (category_id = ANY($6::int[]))),
	_2 AS (
		INSERT INTO doc_categories (doc_id, category_id)
			SELECT $1 doc_id, * FROM unnest($6::int[]) AS category_id
			ON CONFLICT DO NOTHING
	)
UPDATE docs SET
	title = v.title, text = v.text, action_url = v.action_url, source_url = v.source_url
	FROM (VALUES ($1, $2, $3, $4, $5)) as v(id, title, text, action_url, source_url)
	WHERE docs.id = v.id
'''
SQL_DELETE_DOC_PREPROCESSINGS = '''
WITH _ AS (DELETE FROM doc_search_preprocessings WHERE doc_id = ANY ($1::int[]))
DELETE FROM doc_embeddings em USING docs
	WHERE embedding_trunc_id = em.id AND docs.id = ANY ($1::int[])
'''


class NoIdDocument(TypedDict):
    title: Optional[str]
    text: str
    action_url: Optional[str]
    source_url: Optional[str]
    categories: list[int]


class Document(NoIdDocument):
    id: int


class Documents:
    util_client: utility_client.Client
    db_pool: asyncpg.Pool

    def __init__(self, util_client: utility_client.Client, db_pool: asyncpg.Pool):
        self.util_client = util_client
        self.db_pool = db_pool

    async def create(self, docs: list[NoIdDocument]) -> list[int]:
        query_args = map(_to_tuple, docs)
        inserted: list[asyncpg.Record] = await self.db_pool.fetchmany(SQL_INSERT_DOCS, query_args)
        return list(map(itemgetter("id"), inserted))

    async def get(self, doc_ids: list[int]) -> list[Document]:
        records: list[asyncpg.Record] = await self.db_pool.fetch(SQL_GET_DOCS, doc_ids)
        # noinspection PyTypeChecker
        return list(map(dict, records))

    async def get_by_category_ids(self, *, required: list[int] = None, any_of: list[int] = None) -> list[Document]:
        """None or empty means selecting from all records"""
        required = required or []
        any_of = any_of or []
        records: list[asyncpg.Record] = await self.db_pool.fetch(SQL_GET_DOCS_BY_CAT, required, any_of)
        filtered_ids = list(map(itemgetter(0), records))
        return await self.get(filtered_ids)

    async def update(self, update_fn: Callable[[Document], Document], doc_ids: list[int]):
        docs = await self.get(doc_ids)
        updated_docs = map(_to_tuple, map(update_fn, docs))  # without ids
        updated_docs = map(operator.add, zip(doc_ids), updated_docs)  # now with id as first elem
        await self.db_pool.executemany(SQL_UPDATE_DOCS, updated_docs)

    async def delete(self, doc_ids: list[int]):
        await self.db_pool.execute("DELETE FROM docs WHERE id = ANY($1)", doc_ids)

    async def add_category(self, title: str) -> int:
        return await self.db_pool.fetchval("INSERT INTO categories VALUES (DEFAULT, $1) RETURNING id", title)

    async def get_categories(self) -> list[TypedDict("C", {"id": int, "title": str})]:
        records: list[asyncpg.Record] = await self.db_pool.fetch("SELECT id, title FROM categories")
        # noinspection PyTypeChecker
        return list(map(dict, records))

    async def delete_categories(self, category_ids: list[int]):
        await self.db_pool.execute("DELETE FROM categories WHERE id = ANY($1)", category_ids)

    async def delete_preprocessings(self, doc_ids: list[int]):
        await self.db_pool.execute(SQL_DELETE_DOC_PREPROCESSINGS, doc_ids)

    async def run_search_preprocessing(self):
        print("Starting search preprocessing tasks")
        await preprocessing.start_vector_search_preprocessing(self.util_client, self.db_pool)
        await preprocessing.start_fulltext_search_preprocessing(self.db_pool)


def _to_tuple(doc: NoIdDocument) -> (Optional[str], str, Optional[str], Optional[str], Optional[list[int]]):
    return (
        doc.get("title"), doc.get("text"), doc.get("action_url"), doc.get("source_url"), doc.get("categories")
    )


_type_adapters: dict[str, pydantic.TypeAdapter] = {}


def _validate(obj: Any, obj_type: type):
    global _type_adapters
    type_name = str(obj_type)
    validator = _type_adapters.get(type_name)
    if not validator:
        type_adapter = pydantic.TypeAdapter(obj_type)
        _type_adapters[type_name] = type_adapter
        validator = _type_adapters[type_name]
    validator.validate_python(obj)
