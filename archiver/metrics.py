from prometheus_client import Counter

COMMON_LABELNAMES = (
    "name",
    "url",
)

HUEY_TASK_SIGNALS = Counter(
    name="huey_task_signals",
    documentation="Huey task signals.",
    labelnames=COMMON_LABELNAMES + ("signal", "exc_type"),
)
