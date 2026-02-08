import logging


def setup_logging(level: int = logging.INFO, fmt: str | None = None) -> None:
    """
    Configure root logging with sensible defaults.

    Idempotent: calling multiple times will not duplicate handlers.
    """

    if fmt is None:
        fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"

    root = logging.getLogger()
    if root.handlers:
        root.setLevel(level)
        return

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt))
    root.addHandler(handler)
    root.setLevel(level)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
