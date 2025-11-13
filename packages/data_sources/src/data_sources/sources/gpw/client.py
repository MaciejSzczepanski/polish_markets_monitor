import polars as pl
from bs4 import BeautifulSoup
from ...config.http_config import HttpConfig
from ...utils import HttpClient
from time import sleep


class GpwSource:
    def __init__(self, http_config: HttpConfig = None):
        if http_config:
            self.http = HttpClient(http_config)
        else:
            self.http = HttpClient()

    def _standardize_schema(self, df: pl.DataFrame, is_price: bool = True) -> pl.DataFrame:
        """
        Args:
            df
            is_price True if passed df contains stock data, False if contains WIG[20] index rating
        Transform API response to standardized schema

        Renames columns and casts timestamp(int) to datetime

        """
        column_names = {'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume', 'p': 'price',
                        't': 'datetime'}
        if not is_price:
            column_names['p'] = 'index_value'
        df = (df.with_columns(t=(pl.col('t') * 1000).
                              cast(pl.Datetime(time_unit="ms", time_zone="Europe/Warsaw"))).
              rename(column_names, strict=False
                     ))
        return df

    def fetch_metadata(self, company_isin: str = 'PLPKO0000016'):
        """fetch metadata about company
        Raises:
            requests.exceptions.RequestException"""
        infotab_response = self.http.get(
            'https://www.gpw.pl/ajaxindex.php?start=infoTab&format=html&action=GPWListaSp&gls_isin=' +
            company_isin)
        sleep(2)
        indicators_response = self.http.get(
            'https://www.gpw.pl/ajaxindex.php?start=indicatorsTab&format=html&action=GPWListaSp&gls_isin=' +
            company_isin)
        sleep(2)
        company_profile_response = self.http.get('https://www.gpw.pl/spolka?isin=' + company_isin)

        infotab_html = BeautifulSoup(infotab_response.text, 'html.parser')
        indicators_html = BeautifulSoup(indicators_response.text, 'html.parser')
        profile_html = BeautifulSoup(company_profile_response.text, 'html.parser')
        description = profile_html.select_one('.bg_lightGrey+ div').get_text().strip()

        profile_attrs = {key: val.get_text().strip() for attr, val in
                         zip(infotab_html.select('th'), infotab_html.select('td')) if
                         (key := attr.get_text().strip()) in ['Na giełdzie od:', 'Nazwa:', 'Skrót:', 'Nazwa pełna:',
                                                              'Adres siedziby:',
                                                              'Województwo:', 'Strona www:']}
        sector = {key.get_text().strip(): val.get_text().strip() for key, val in
                  zip(indicators_html.select('th'), indicators_html.select('td'))}.get('Sektor')
        key_mapping = {
            'Na giełdzie od:': 'listed_since',
            'Nazwa:': 'name',
            'Skrót:': 'ticker',
            'Nazwa pełna:': 'full_name',
            'Adres siedziby:': 'headquarters_address',
            'Województwo:': 'voivodeship',
            'Strona www:': 'website'
        }
        output = {key_mapping.get(key): profile_attrs.get(key) for key in key_mapping.keys()}
        output['description'] = description
        output['company_isin'] = company_isin
        output['sector'] = sector
        return output

    def fetch_company_history_data(self, company_isin: str) -> pl.DataFrame:
        """fetch history ohlc + volume data (daily records) for company
        Raises:
            requests.exceptions.RequestException"""
        response = self.http.get(
            f'https://www.gpw.pl/chart-json.php?req=[{{%22isin%22:%22{company_isin}%22,%22mode%22:%22ARCH%22,%22from%22:null,%22to%22:null}}]')
        data = response.json()
        data = pl.DataFrame(data, strict=False)
        data = data.explode('data').select(pl.col('data')).unnest('data')
        data = data.with_columns(isin=pl.lit(company_isin))
        data = self._standardize_schema(data)
        return data

    def fetch_ohlc(self, company_isin: str) -> pl.DataFrame:
        """fetch open/high/low/close + volume data in current day, safe to use after ~ 17:30 UTC
         Raises:
            requests.exceptions.RequestException
            KeyError: unexcepted format of returned data
            polars.exception.PolarsException
         """
        response = self.http.get(
            f'https://www.gpw.pl/chart-json.php?req=%5B%7B%22isin%22:%22{company_isin}%22,%22mode%22:%22CURR%22,%22from%22:1%7D%5D'
        )
        data = pl.DataFrame(response.json()[0]['data'])
        data = data.with_columns(isin=pl.lit(company_isin))
        data = self._standardize_schema(data)
        return data

    def fetch_wig20(self):
        """fetch today wig20 rating"""
        response = self.http.get(
            'https://gpwbenchmark.pl/chart-json.php?req=[{%22isin%22:%22PL9999999987%22,%22mode%22:%22CURR%22,%22from%22:1}]')
        data = pl.DataFrame(response.json()[0]['data'])
        data = self._standardize_schema(data, is_price=False)
        return data

    def fetch_all_wig20_isin(self):
        """fetch isin of all companies in WIG20"""
        response = self.http.get(
            'https://gpwbenchmark.pl/ajaxindex.php?action=GPWIndexes&start=ajaxPortfolio&format=html&lang=PL&isin=PL9999999987&cmng_id=1010&time=1760035482399')
        html = BeautifulSoup(response.text, 'html.parser')
        isins = [x.get_text() for x in html.select('td:nth-child(2)')]
        return isins
