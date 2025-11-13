from ...config.http_config import HttpConfig
from ...utils import HttpClient
from ...utils.rss import RSSFeed
import polars as pl
from datetime import date, datetime, timedelta
from bs4 import BeautifulSoup

class InteriaSource:
    def __init__(self, http_config: HttpConfig = None):

        if http_config:
            self.http = HttpClient(http_config)
        else:
            self.http = HttpClient()

        self.news = RSSFeed('https://biznes.interia.pl/feed')
        self.name = 'interia'

    def fetch_news(self) -> pl.DataFrame:
        """Fetch today news from RSS bankier.pl
        Raises:
            utils.rss.NoDataAvailable: when no news available"""
        df = self.news.fetch_feed()
        df = df.filter(pl.col('date').dt.date() == date.today())
        return df

    def fetch_news_content(self, link):
        """Fetch news content"""
        html = self.http.get(link)
        soup = BeautifulSoup(html.text, 'lxml')

        content = soup.select_one('article.article-container')
        '\n'.join([p.get_text() for p in content.select('.ids-paragraph--lead , .ids-paragraph--default span')])
        title = soup.select_one('.ids-article-header--medium').get_text()
        date = soup.select_one('time:nth-child(1) , .ids-article-header--medium').get_text().strip()

        return {'title': title, 'date': date, 'content': content}

    def _parse_date(self, publication_date:str) -> datetime:
        # Hardcoded month names to avoid server locale issues.
        month_mapping = {
            "stycznia": 1,
            "lutego": 2,
            "marca": 3,
            "kwietnia": 4,
            "maja": 5,
            "czerwca": 6,
            "lipca": 7,
            "sierpnia": 8,
            "września": 9,
            "października": 10,
            "listopada": 11,
             "grudnia": 12
        }

        if polish_month:=[(month_mapping[key], key) for key in month_mapping.keys() if key in publication_date]:
            # polish month is in date format
            polish_month = polish_month[0]
            publication_date = publication_date.replace(polish_month[1], str(polish_month[0]))
            return datetime.strptime(publication_date, '%d %m %Y %H:%M')

        if (minute:=publication_date.split(' ')[0]).isdigit():
            # date format is like 40 minut temu or %d %m %Y
            minute = int(minute)
            publication_date = datetime.now() - timedelta(minutes=minute)
            return publication_date

        if (date_parts:= publication_date.split(','))[0] in ['Dzisiaj', 'Wczoraj']:
            # data startswith Dzisiaj/Wczoraj
            hour = date_parts[1].split(':')[0]
            hour = int(hour)
            minute = date_parts[1].split(':')[1]
            minute = int(minute)
            td = timedelta(hours=int(hour), minutes=int(minute))

            if date_parts == 'Dzisiaj':
                publication_date = datetime.today().replace(hour=hour, minute=minute)
            else: # Wczoraj
                publication_date = datetime.today() - timedelta(days=1) + timedelta(hours=hour, minutes=minute)
            return publication_date

        else:
            raise ValueError('Not known data format')

