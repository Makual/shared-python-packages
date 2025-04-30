from asyncio import create_task
from types import MappingProxyType

import aio_pika
import aio_pika.abc as pika_types
from doc_search_api import message_types as msg_t

from . import defaults
from . import message_validation
from . import task_sender

QUERY_EXPANSION_QUEUE_NAME = "doc-search-api-tasks.query-expansion"
DOCUMENTS_FETCHING_QUEUE_NAME = "doc-search-api-tasks.documents-fetching"
CANDIDATES_RETRIEVAL_FROM_QE_QUEUE_NAME = "doc-search-api-tasks.candidates-retrieval"
CANDIDATES_RETRIEVAL_FROM_MESSAGES_QUEUE_NAME = "doc-search-api-tasks.candidates-retrieval-from-messages"
RANKED_RESULTS_FROM_CANDIDATES_QUEUE_NAME = "doc-search-api-tasks.ranked-results-from-candidates"
RANKED_RESULTS_FROM_MESSAGES_QUEUE_NAME = "doc-search-api-tasks.ranked-results-from-messages"
GENERATE_SHORT_ANSWERS_FROM_RANKED_RESULTS_QUEUE_NAME = "doc-search-api-tasks.generate-short-answers-from-ranked-results"
GENERATE_SHORT_ANSWERS_QUEUE_NAME = "doc-search-api-tasks.generate-short-answers"


class Client:
    channel: pika_types.AbstractChannel
    conf: task_sender.Config

    def __init__(self, channel: pika_types.AbstractChannel, conf: task_sender.Config = MappingProxyType({})):
        self.channel = channel
        self.conf = conf

    async def query_expansion(self, messages: list[msg_t.ChatMessage]) -> msg_t.QueryExpansionResult:
        message_validation.validate(messages, list[msg_t.ChatMessage])
        return await task_sender.add_task_to_queue(
            QUERY_EXPANSION_QUEUE_NAME, {"messages": messages}, self.channel, self.conf,
        )

    async def documents_fetching(self, doc_ids: list[int]) -> msg_t.DocumentsFetchingResult:
        message_validation.validate(doc_ids, list[int])
        return await task_sender.add_task_to_queue(
            DOCUMENTS_FETCHING_QUEUE_NAME, {"doc_ids": doc_ids}, self.channel, self.conf,
        )

    async def candidates_retrieval(
            self,
            query_expansion: msg_t.QueryExpansionResult,
            result_count: int = None,
            required_categories: list[str] = None,  # None or empty means retrieve from entire knowledge base
    ) -> msg_t.CandidatesRetrievalResult:
        task: msg_t.CandidatesRetrievalTask = {
            "query_expansion": query_expansion,
            "result_count": result_count,
            "required_categories": required_categories,
        }
        message_validation.validate(task, msg_t.CandidatesRetrievalTask)
        return await task_sender.add_task_to_queue(
            CANDIDATES_RETRIEVAL_FROM_QE_QUEUE_NAME, task, self.channel, self.conf,
        )

    async def candidates_retrieval_from_messages(
            self,
            messages: list[msg_t.ChatMessage],
            result_count: int = None,
            required_categories: list[str] = None,  # None or empty means retrieve from entire knowledge base
    ) -> msg_t.CandidatesRetrievalResult:
        task: msg_t.CandidatesFromMessagesRetrievalTask = {
            "messages": messages,
            "result_count": result_count,
            "required_categories": required_categories,
        }
        message_validation.validate(task, msg_t.CandidatesFromMessagesRetrievalTask)
        return await task_sender.add_task_to_queue(
            CANDIDATES_RETRIEVAL_FROM_MESSAGES_QUEUE_NAME, task, self.channel, self.conf,
        )

    async def ranked_results_from_candidates(
            self,
            candidates_retrieval_result: msg_t.CandidatesRetrievalResult,
    ) -> msg_t.RankedResultsResult:
        task: msg_t.RankedResultsFromSearchResultsTask = candidates_retrieval_result
        message_validation.validate(task, msg_t.RankedResultsFromSearchResultsTask)
        return await task_sender.add_task_to_queue(
            RANKED_RESULTS_FROM_CANDIDATES_QUEUE_NAME, task, self.channel, self.conf,
        )

    async def ranked_results_from_messages(
            self,
            messages: list[msg_t.ChatMessage],
            result_count: int = None,
            required_categories: list[str] = None,  # None or empty means retrieve from entire knowledge base
    ) -> msg_t.RankedResultsResult:
        task: msg_t.CandidatesFromMessagesRetrievalTask = {
            "messages": messages,
            "result_count": result_count,
            "required_categories": required_categories,
        }
        message_validation.validate(task, msg_t.CandidatesFromMessagesRetrievalTask)
        return await task_sender.add_task_to_queue(
            RANKED_RESULTS_FROM_MESSAGES_QUEUE_NAME, task, self.channel, self.conf,
        )


async def query_expansion(messages: list[msg_t.ChatMessage]) -> msg_t.QueryExpansionResult:
    return await (await get_default_client()).query_expansion(messages)


async def documents_fetching(doc_ids: list[int]) -> msg_t.DocumentsFetchingResult:
    return await (await get_default_client()).documents_fetching(doc_ids)


async def candidates_retrieval(
        query_expansion: msg_t.QueryExpansionResult,
        result_count: int = None,
        required_categories: list[str] = None,
) -> msg_t.CandidatesRetrievalResult:
    return await (await get_default_client()).candidates_retrieval(query_expansion, result_count, required_categories)


async def candidates_retrieval_from_messages(
        messages: list[msg_t.ChatMessage],
        result_count: int = None,
        required_categories: list[str] = None,
) -> msg_t.CandidatesRetrievalResult:
    return await (await get_default_client()).candidates_retrieval_from_messages(
        messages, result_count, required_categories
    )


async def ranked_results_from_candidates(
        candidates_retrieval_result: msg_t.CandidatesRetrievalResult
) -> msg_t.RankedResultsResult:
    return await (await get_default_client()).ranked_results_from_candidates(candidates_retrieval_result)


async def ranked_results_from_messages(
        messages: list[msg_t.ChatMessage],
        result_count: int = None,
        required_categories: list[str] = None,
) -> msg_t.RankedResultsResult:
    return await (await get_default_client()).ranked_results_from_messages(messages, result_count, required_categories)


async def get_default_client() -> Client:
    async def create_client():
        rabbit_conn = await aio_pika.connect_robust(**defaults.DEFAULT_CONNECTION_CONF)
        channel = await rabbit_conn.channel()
        return Client(channel, defaults.DEFAULT_TASK_SENDER_CONF)

    if not '_default_client' in globals():
        _default_client = create_task(create_client())
    return await _default_client
