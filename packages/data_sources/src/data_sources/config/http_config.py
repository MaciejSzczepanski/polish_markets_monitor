from dataclasses import dataclass


@dataclass
class HttpConfig:
    proxy: str = None
    user_agent: str = None
    timeout: int = 30
    max_retries: int = 3
    waiting_factor: float = 1.0
    headers: dict[str, str] = None
    proxy_error_wait: int = 1
    timeout_wait: int = 60
    connection_error_wait: int = 5
    log_level: str = None
