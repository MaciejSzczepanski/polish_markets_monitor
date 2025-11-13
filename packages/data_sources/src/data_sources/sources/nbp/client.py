import datetime
import logging
from functools import wraps
from typing import Literal
import requests
import polars as pl
from ...config.http_config import HttpConfig
from ...utils import HttpClient, camel_to_snake

CurrencyType = Literal['mid_market_rate', 'mid_market_rate_unpopular', 'bid_ask']


class NoDataAvailableError(Exception):
    """Raised when API returns 400 - no data for requsted date"""
    pass


class NbpSource:

    def __init__(self, http_config: HttpConfig = None):
        if http_config:
            self.http = HttpClient(http_config)
        else:
            self.http = HttpClient()

        self.logger = logging.getLogger('sources.nbp.client')
        self.base_url = "https://api.nbp.pl/api/"

    @staticmethod
    def handle_daterange(days_constraint:int = None):
        """decorator which parses dates and does simple date range validation
        Raises
            ValueError: when data range is invalid"""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):

                if isinstance(kwargs['date_from'], str) and  isinstance(kwargs['date_to'], str):
                    kwargs['date_from'] = datetime.date.fromisoformat(kwargs['date_from'])
                    kwargs['date_to'] = datetime.date.fromisoformat(kwargs['date_to'])

                if kwargs['date_to'] < kwargs['date_from']:
                    raise ValueError('date_from must be before date_to')

                if days_constraint and (kwargs['date_to'] - kwargs['date_from']).days > days_constraint:
                    raise ValueError(f'Date range > {days_constraint} days')

                return func(*args, **kwargs)

            return wrapper
        return decorator



    def _fetch(self, endpoint: str, date_from: datetime.date | None = None,
               date_to: datetime.date | None = None) -> requests.Response:
        """
        Raises:
            requests.exceptions.RequestException
        """

        url = self.base_url + endpoint

        if date_from and date_to:
            url += f'/{date_from.isoformat()}/{date_to.isoformat()}'

        url += '/?format=json'
        response = self.http.get(url)
        return response

    def _fetch_currencies(
            self, endpoint: str, curr_type, date_from: datetime.date | None = None,
            date_to: datetime.date | None = None
    ) -> requests.Response:
        """helper method for fetching currencies data
        Raises:
            requests.exceptions.RequestException
            NoDataAvailableError: when no data is available
            ValueError: when date range is invalid
        """
        types = {'mid_market_rate': 'A',  # mapping to NBP endpoints
                 'mid_market_rate_unpopular': 'B',  #
                 'bid_ask': 'C'}
        url = f'exchangerates/tables/{types.get(curr_type)}/{endpoint}'
        response = self._fetch(url, date_from, date_to)
        return response

    def fetch_currencies_actual(self, curr_type: CurrencyType) -> requests.Response:
        """fetches current currencies data
        Raises
            requests.exceptions.RequestException"""
        return self._fetch_currencies(endpoint='', curr_type=curr_type)

    @handle_daterange(days_constraint=93)
    def fetch_currencies_daterange(self, *, curr_type: CurrencyType, date_from: datetime.date,
                                   date_to: datetime.date
                                   ) -> requests.Response:
        """fetching value of currencies in given date range
        Raises:
            ValueError: wrong date range, max date range is 93 days
            requests.exceptions.RequestException"""
        return self._fetch_currencies('', curr_type=curr_type, date_from=date_from,
                                      date_to=date_to)

    def fetch_currencies_today(self, curr_type: CurrencyType) -> requests.Response:
        """fetching currenciences prices for today
        Raises:
            NoDataAvailableError:
            requests.exceptions.RequestException"""
        response = self._fetch_currencies(endpoint='today', curr_type=curr_type)
        if response.status_code == 404:
            raise NoDataAvailableError("No data for today available or no working day")
        else:
            return response

    def _fetch_gold(self, endpoint: str, date_from: datetime.date | None = None,
                    date_to: datetime.date = None
                    ) -> requests.Response:
        """ helper function to fetch gold prices
        Raises:
            requests.exceptions.RequestException
        """
        url = f'cenyzlota/' + endpoint
        return self._fetch(url, date_from = date_from, date_to = date_to)

    def fetch_gold_today(self) -> requests.Response:
        """fetching today gold pricing
        Raises:
         requests.exceptions.RequestException
         NoDataAvailableError"""

        response = self._fetch_gold('today')

        if response.status_code == 400:
            raise NoDataAvailableError("data hasn't been publicated yet or no working day")
        return response

    @handle_daterange(days_constraint=367)
    def fetch_gold_datarange(self, *, date_from: str | datetime.date, date_to: str | datetime.date
                             ) -> requests.Response:
        """fetching gold pricing in given date range
        Raises:
            requests.exceptions.RequestException
            """
        return self._fetch_gold('', date_from=date_from, date_to=date_to)

    def fetch_gold_actual(self) -> requests.Response:
        """fetching current gold pricing
        Raises:
            requests.exceptions.RequestException
            """
        return self._fetch_gold(endpoint='')

    def transform_currency(self, data: requests.Response) -> pl.DataFrame:
        """ transform nbp api answer to DataFrame
        Raises:
            polars.exceptions.PolarsError
            json.decoder.JSONDecodeError
        """
        df = pl.DataFrame(data.json())
        df = df.explode("rates")
        df = df.unnest('rates')
        df.columns = [camel_to_snake(s) for s in df.columns]
        df = df.with_columns(pl.col("^.*_date$").str.to_date(format="%Y-%m-%d"))
        return df

