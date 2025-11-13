import requests
import logging
from time import sleep
from ..config.http_config import HttpConfig


class HttpClient:
    def __init__(self, config: HttpConfig = HttpConfig()):
        self.config = config
        self.session = requests.Session()
        self._setup_session()
        self.logger = logging.getLogger('http_client')

    def _setup_session(self) -> None:
        if self.config.proxy:
            self.session.proxies.update({'http': self.config.proxy, 'https': self.config.proxy})

        if self.config.headers:
            self.session.headers.update(self.config.headers)

        if self.config.user_agent:
            self.session.headers.update({'User-Agent': self.config.user_agent})

    def _request_with_retry(self, method: str, url: str) -> requests.Response:

        for attempt in range(1, self.config.max_retries + 1):
            try:
                self.logger.info(f"Fetching {url}; attempt {attempt}/{self.config.max_retries}")
                response = self.session.request(method, url, timeout=getattr(self.config, 'timeout', 15))
                response.close()
                should_continue, status_code = self._analyze_http_response(response, attempt=attempt)
                if not should_continue:
                    return response

                if status_code == 'retry_rate_limit':
                    self._should_retry_and_wait(attempt, multiplier=2)
                elif status_code == 'retry_normal':
                    self._should_retry_and_wait(attempt, self.config.waiting_factor)

            except requests.exceptions.ProxyError:
                if attempt < self.config.max_retries:
                    sleep(self.config.proxy_error_wait)
                    continue
                else:
                    raise
            except requests.exceptions.Timeout:
                if attempt < self.config.max_retries:
                    sleep(self.config.timeout_wait)
                    continue
                else:
                    raise
            except requests.exceptions.ConnectionError:
                if attempt < self.config.max_retries:
                    sleep(self.config.connection_error_wait)
                    continue
                else:
                    raise
            except Exception as e:
                logging.error(f'Unexpected error: {str(e)}')
                raise
        return response

    def get(self, url: str) -> requests.Response | None:
        return self._request_with_retry('get', url)

    def _analyze_http_response(self, response, attempt: int) -> tuple[bool, str]:
        """

        """
        if hasattr(response, 'status_code'):
            if response.status_code == 200:
                self.logger.info(f"Fetched {response.url} successfully")
                return False, 'return'
            # server overloaded - pause
            elif response.status_code == 429:
                self.logger.info(f'{response.url} Rate limit(429) - attempt {attempt} / {self.config.max_retries}')
                return True, 'retry_rate_limit'

            elif 400 <= response.status_code < 500:
                logging.error(f'4xx for {response.url} - client error')
                return True, 'return'

            elif 500 <= response.status_code < 600:
                if attempt < self.config.max_retries:
                    return True, 'retry_normal'
                else:
                    self.logger.error(f'Exceeded number of max retries for {response.url}, last status code: {response.status_code}')
                    return False, 'return'
            else:
                self.logger.warning(f'Unhandled status code for {response.url}: {response.status_code}')
                return False, 'unhandled_status_code'
        else:
            logging.error(f'Response object has no status code: {response}')
            return False, 'no_status_code'

    def _calculate_sleep_time(self, attempt: int, multiplier: float = 1):
        """Return time in seconds to wait before retrying"""

        return (attempt ** 2) * self.config.waiting_factor * multiplier

    def _should_retry_and_wait(self, attempt: int, multiplier: float = 1) -> bool:
        """Checks whether to retry, if so then wait, return True if retry"""
        if attempt < self.config.max_retries:
            sleep_time = self._calculate_sleep_time(attempt, multiplier)
            sleep(sleep_time)
            return True
        return False

