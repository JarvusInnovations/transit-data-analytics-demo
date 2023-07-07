from prometheus_client import Counter, Summary

COMMON_LABELNAMES = (
    "name",
    "url",
    "feed_type",
)

HUEY_TASK_SIGNALS = Counter(
    name="huey_task_signals",
    documentation="Huey task signals.",
    labelnames=COMMON_LABELNAMES + ("signal", "exc_type"),
)

FETCH_REQUEST_DELAY_SECONDS = Summary(
    name="fetch_request_delay_seconds",
    documentation="Delay before a fetch request is executed.",
    labelnames=COMMON_LABELNAMES,
)

FETCH_REQUEST_DURATION_SECONDS = Summary(
    name="fetch_request_duration_seconds",
    documentation="Duration of just the request for a fetch.",
    labelnames=COMMON_LABELNAMES,
)

FETCH_SAVE_DURATION_SECONDS = Summary(
    name="fetch_save_duration_seconds",
    documentation="Duration of just the save for a fetch.",
    labelnames=COMMON_LABELNAMES,
)
