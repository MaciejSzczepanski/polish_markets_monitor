import feedparser
from bs4 import BeautifulSoup
import polars as pl
from ..config.company_mappings import WIG20
from ..config.http_config import HttpConfig
from ..utils import HttpClient

class NoDataAvailable(Exception):
    """Exception raised when no news available"""
    pass

class RSSFeed:
    def __init__(self, url: str, http_config: HttpConfig = None, company_mapping: dict = WIG20):
        """
        Args:
            url: RSS feed url
            http_config: Optional HTTP client config
            company_mapping: List of dicts with 'isin' and 'phrases' keys for company matching
        """
        if http_config:
            self.http = HttpClient(http_config)
        else:
            self.http = HttpClient(HttpConfig())
        self.url = url
        self.company_mapping = company_mapping

    def fetch_feed(self) -> pl.DataFrame:
        """Fetch and parse, adding matched company ISINs if mapping provided

        Returns:
            DataFrame with columns: title, link, date, summary, company_isins (if mapping set)
            """

        feed_raw = self.http.get(self.url).text
        feed_parsed = feedparser.parse(feed_raw)
        entries_parsed = []

        for entry in feed_parsed.get('entries'):
            summary = entry.get('summary')
            if summary:
                summary=BeautifulSoup(summary, 'lxml').get_text()

            entries_parsed.append({
                'title': entry.get('title'),
                'link': entry.get('link'),
                'date': entry.get('published'),
                'summary': summary
            })
        if not entries_parsed:
            raise NoDataAvailable("No news available")
        df = pl.DataFrame(entries_parsed)
        df = df.with_columns(date = pl.col('date').str.to_datetime(format="%a, %d %b %Y %H:%M:%S %Z").
                             dt.convert_time_zone('Europe/Warsaw'))

        if self.company_mapping:
            df = self._add_company_matches(df)

        return df

    def _add_company_matches(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add company_isins column with matched ISIN codes from summary text
        Returns:
             DataFrame with added 'company_isins' column containing list of matched ISINs
            """
        mapping = {alias: company['isin'] for company in self.company_mapping for alias in
                   company['phrases']}
        pattern = '|'.join(
            [r'\b' + phrase for company in self.company_mapping for phrase in company['phrases']])
        df = df.with_columns(company_isins=pl.col('summary').str.extract_all(r'(?i)' + pattern). \
                             list.eval(pl.element().replace_strict(mapping, default=None).unique()))
        return df



