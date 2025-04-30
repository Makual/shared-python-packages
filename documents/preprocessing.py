import re
from datetime import datetime
from itertools import batched

import asyncpg
from api_client import utility_client

DATABASE_FETCHING_CHUNK_SIZE = 5  # rows
MAX_CHARS_FOR_VECTORIZER_REQUEST = 1500
SENTENCE_END_REGEXP = re.compile(r'\S.(?:[.?!](?=\s|$)|(?=\s*(?:\n|$)))')

SQL_FT_SEARCH_LAST_PREPROCESSING = \
    """SELECT completed_at FROM preprocessing_journal WHERE "for" = 'fulltext search' ORDER BY completed_at DESC LIMIT 1"""
SQL_FT_SEARCH_FOUND_FOR_PROCESSING = (
    'SELECT COUNT(*) FROM docs d'
    '  LEFT JOIN doc_search_preprocessings s ON d.id = s.doc_id'
    '  WHERE doc_id IS NULL OR updated_at > $1'
)
SQL_FT_SEARCH_REMOVE_OUTDATED = \
    "DELETE FROM doc_search_preprocessings s USING docs d WHERE doc_id = d.id AND updated_at > $1"
SQL_FT_SEARCH_INSERT_FRESH = """
WITH prepared AS (
	SELECT
		d.id,
		to_tsvector('russian', lower(title)),
		to_tsvector('russian', lower(text))
	FROM docs d
	LEFT JOIN doc_search_preprocessings s ON d.id = s.doc_id
	WHERE doc_id IS NULL OR updated_at > $1
)
INSERT INTO doc_search_preprocessings (doc_id, title_vector, text_vector)
	SELECT * FROM prepared
"""
SQL_FT_SEARCH_INSERT_JOURNAL = """INSERT INTO preprocessing_journal ("for") VALUES ('fulltext search')"""

SQL_V_SEARCH_LAST_PREPROCESSING = \
    """SELECT completed_at FROM preprocessing_journal WHERE "for" = 'vector search' ORDER BY completed_at DESC LIMIT 1"""
SQL_V_SEARCH_FOUND_FOR_PROCESSING = \
    'SELECT COUNT(*) FROM docs WHERE embedding_trunc_id IS NULL OR updated_at > $1'
SQL_V_SEARCH_SELECT_DOCS = \
    'SELECT id, title, text, updated_at FROM docs WHERE embedding_trunc_id IS NULL OR updated_at > $1'
SQL_V_SEARCH_DELETE_EMBEDDING = \
    'DELETE FROM doc_embeddings e USING docs d WHERE embedding_trunc_id = e.id AND d.id = {doc_id}'
SQL_V_SEARCH_INSERT_EMBEDDING = \
    'INSERT INTO doc_embeddings (embedding) VALUES ($1::double precision[]) RETURNING id'
SQL_V_SEARCH_UPDATE_EMBEDDING = \
    'UPDATE docs SET embedding_trunc_id={embedding_id} WHERE id={doc_id}'
SQL_V_SEARCH_INSERT_JOURNAL = \
    """INSERT INTO preprocessing_journal ("for", completed_at) VALUES ('vector search', now() + INTERVAL '1 SECOND')"""


async def start_fulltext_search_preprocessing(db_conn: asyncpg.Pool):
    print("Start preprocessing for fulltext search")
    last_preprocessing_at: datetime = \
        await db_conn.fetchval(SQL_FT_SEARCH_LAST_PREPROCESSING) or datetime.fromtimestamp(0)
    print("Last preprocessing was", last_preprocessing_at or "never done")
    total = await db_conn.fetchval(SQL_FT_SEARCH_FOUND_FOR_PROCESSING, last_preprocessing_at)
    print(total, "docs found for preprocessing")
    await db_conn.execute(SQL_FT_SEARCH_REMOVE_OUTDATED, last_preprocessing_at)
    await db_conn.execute(SQL_FT_SEARCH_INSERT_FRESH, last_preprocessing_at)
    await db_conn.execute(SQL_FT_SEARCH_INSERT_JOURNAL)
    print("Preprocessing for fulltext search completed")


async def start_vector_search_preprocessing(
        util_client: utility_client.Client,
        db_conn: asyncpg.Pool,
):
    print("Start preprocessing for vector search")
    last_preprocessing_at: datetime = \
        await db_conn.fetchval(SQL_V_SEARCH_LAST_PREPROCESSING) or datetime.fromtimestamp(0)
    print("Last preprocessing was", last_preprocessing_at or "never done")
    count = 0
    docs_for_processing = await db_conn.fetch(SQL_V_SEARCH_SELECT_DOCS, last_preprocessing_at)
    print(len(docs_for_processing), "docs found for preprocessing")
    doc_chunk: tuple[asyncpg.Record, ...]
    for doc_chunk in batched(docs_for_processing, DATABASE_FETCHING_CHUNK_SIZE):
        count += len(doc_chunk)
        print(f"Processing {len(doc_chunk)} docs...")
        vectorizer_input_list = []
        for doc in doc_chunk:
            prepared_doc = f'{doc["title"]}\n' if doc["title"] else ""
            prepared_doc += doc["text"]
            prepared_doc = _trim_by_sentence(prepared_doc, MAX_CHARS_FOR_VECTORIZER_REQUEST)
            vectorizer_input_list.insert(0, prepared_doc)
        embeddings = await util_client.get_embedding(vectorizer_input_list)
        assert len(embeddings) == len(doc_chunk), (
            "Something wrong with received embeddings list - length of the list does not match the length of the chunk:\n"
            f"len(embeddings) = {len(embeddings)}, len(doc_chunk) = {len(doc_chunk)}"
        )
        print("DONE. Embeddings received, inserting into the table...")
        for i, embedding in enumerate(embeddings):
            doc_id = doc_chunk[i]["id"]
            await db_conn.execute(SQL_V_SEARCH_DELETE_EMBEDDING.format(doc_id=doc_id))
            embedding_id = await db_conn.fetchval(SQL_V_SEARCH_INSERT_EMBEDDING, embedding)
            await db_conn.execute(SQL_V_SEARCH_UPDATE_EMBEDDING.format(doc_id=doc_id, embedding_id=embedding_id))
        print(f"DONE {count} of {len(docs_for_processing)}.")
    await db_conn.execute(SQL_V_SEARCH_INSERT_JOURNAL)
    print("Preprocessing for vector search completed")


def _index_of_sentence_end(text):
    match = SENTENCE_END_REGEXP.search(text)
    if match:
        return match.end()
    else:
        return len(text)


def _trim_by_sentence(text, max_chars) -> str:
    if len(text) <= max_chars:
        return text
    first_sentence = _index_of_sentence_end(text)
    is_possible = True if first_sentence <= max_chars else False
    assert is_possible, "can't trim the text: " + text[:30] + "..."
    pos = 0
    while len(rest_text := text[pos:]) > 0:
        if pos == max_chars:
            break
        next_pos = pos + _index_of_sentence_end(rest_text)
        if next_pos > max_chars:
            break
        pos = next_pos
    return text[:pos]