"""Classification + backoff for transient SQL Server errors."""

# Network-layer transients: TCP reset, login timeout, dropped connection.
# Almost always recover within seconds.
_CONNECTION_CODES = (
    "08S01",
    "08001",
    "10054",
    "10053",
    "Communication link failure",
    "connection was forcibly closed",
)

# Lock-contention transients: LOCK_TIMEOUT fired (typically Sch-S vs Sch-M
# from concurrent DDL like ALTER, DROP, CREATE INDEX). Often takes minutes
# to clear, so they get a longer back-off ramp.
_LOCK_CODES = (
    "1222",
    "Lock request time out period exceeded",
)


def is_lock_timeout(err_str: str) -> bool:
    return any(code in err_str for code in _LOCK_CODES)


def is_transient(err_str: str) -> bool:
    return any(code in err_str for code in _CONNECTION_CODES) or is_lock_timeout(err_str)


def backoff_seconds(err_str: str, attempt: int, base: int = 30) -> int:
    """Exponential back-off, with a longer ramp for lock-contention errors.

    Connection errors:    base * 2^attempt   (default 30, 60, 120, 240 ...)
    Lock-timeout errors:  base * 4^attempt   (default 30, 120, 480, ...)
    """
    if is_lock_timeout(err_str):
        return base * (4 ** attempt)
    return base * (2 ** attempt)
