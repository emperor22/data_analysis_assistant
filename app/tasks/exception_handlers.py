
from app.exceptions import (
    BlacklistedDatasetException,
    RetryableRateLimitException,
    TerminalRateLimitException,
    ModelNotFoundException,
)

from app.crud import (
    PromptTableOperation,
    BlacklistedDatasetsTableOperation
)

from app.schemas import (
    TaskStatus
)


from app.logger import logger
from app.config import Config


def handle_validation_error_prompt_task(
    error,
    user_id,
    request_id,
    resp,
    dataset_id,
    blacklist_table_ops: BlacklistedDatasetsTableOperation,
    prompt_table_ops: PromptTableOperation,
):
    if blacklist_table_ops.check_if_blacklisted(dataset_id) is None:
        blacklist_table_ops.add_dataset_to_table(dataset_id)

    if (
        blacklist_table_ops.get_failed_attempt_count(dataset_id)
        < Config.FAILED_ATTEMPT_THRESHOLD_FOR_BLACKLIST
    ):
        blacklist_table_ops.increment_failed_attempt(dataset_id)
        logger.warning(
            f"failed llm response validation: request_id {request_id}, user_id {user_id}, resp -> {resp}"
        )
        logger.exception(error)
        raise error

    prompt_table_ops.change_request_status_sync(
        request_id=request_id,
        status=TaskStatus.failed_because_blacklisted_dataset.value,
    )
    blacklist_table_ops.mark_as_blacklisted(dataset_id)

    logger.warning(
        f"dataset blacklisted: dataset_id {dataset_id}, request_id {request_id}"
    )

    raise BlacklistedDatasetException


def handle_rate_limit_exception_prompt_task(
    user_id, request_id, prompt_table_ops: PromptTableOperation
):
    if (
        prompt_table_ops.check_rate_limit_retry_count(user_id, request_id)
        < Config.RATE_LIMIT_RETRY_COUNT_CAP
    ):
        logger.warning(
            f"retrying due to rate limit: request_id {request_id}, user_id {user_id}"
        )
        prompt_table_ops.increment_rate_limit_retry_count_sync(user_id, request_id)
        raise RetryableRateLimitException

    prompt_table_ops.change_request_status_sync(
        request_id=request_id, status=TaskStatus.failed_because_rate_limited.value
    )
    prompt_table_ops.reset_rate_limit_retry_count_sync(
        user_id=user_id, request_id=request_id
    )

    logger.warning(
        f"request hit llm endpoint rate limit: request_id {request_id}, user_id {user_id}"
    )

    raise TerminalRateLimitException


def handle_model_not_found_exception(
    request_id, prompt_table_ops: PromptTableOperation
):
    prompt_table_ops.change_request_status_sync(
        request_id=request_id, status=TaskStatus.failed_because_model_not_found.value
    )
    raise ModelNotFoundException