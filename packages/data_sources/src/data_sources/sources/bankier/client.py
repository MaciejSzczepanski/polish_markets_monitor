from ...config.http_config import HttpConfig
from ...utils import HttpClient
from ...utils.rss import RSSFeed
import polars as pl
from datetime import date, datetime, timezone
from bs4 import BeautifulSoup

class BankierSource:
    def __init__(self, http_config: HttpConfig = None):

        if http_config:
            self.http = HttpClient(http_config)
        else:
            self.http = HttpClient()

        self.name = 'bankier'
        self.general_news = RSSFeed('https://www.bankier.pl/rss/wiadomosci.xml')
        self.stock_news = RSSFeed('https://www.bankier.pl/rss/gielda.xml')

    def fetch_stock_news(self) -> pl.DataFrame:
        """Fetch today stock news from RSS bankier.pl"""
        df = self.stock_news.fetch_feed()
        df = df.filter(pl.col('date').dt.date() == date.today())
        return df

    def fetch_news(self) -> pl.DataFrame:
        """Fetch today news from RSS bankier.pl"""
        df = self.general_news.fetch_feed()
        df = df.filter(pl.col('date').dt.date() == date.today())
        return df

    def fetch_news_content(self, link):
        """Scrape news from given link at Bankier.pl"""
        html = self.http.get(link)
        soup = BeautifulSoup(html.text, 'lxml')
        news_content = soup.select('.o-article-content p')
        news_content = '\n'.join([p.get_text() for p in news_content])
        header = soup.select_one('.o-article-header')
        title = header.select_one('.-blue').get_text()
        publication_date = soup.select_one('.-md-visible .a-span').get_text()
        publication_date = datetime.strptime(publication_date, '%Y-%m-%d %H:%M')
        return {'title': title, 'date': publication_date, 'content': news_content}


