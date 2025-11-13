import datetime
import io
import os
from typing import Annotated
from fastapi import FastAPI, HTTPException, Depends, Query
from dotenv import load_dotenv
from fastapi.responses import StreamingResponse
from data_access import DuckS3

load_dotenv()
app = FastAPI()


def get_ducks3() -> DuckS3:
    return DuckS3(bucket=os.getenv("S3_BUCKET"), )


def last_date_of_ohlc_data():
    """Returns the last date of OHLC data available in S3."""
    s3 = get_ducks3()
    with s3.get_connection() as conn:
        last_date = conn.sql(f"""WITH dates AS(
        SELECT * FROM read_parquet('{s3.s3}/ohlc/**/*.parquet', hive_partitioning=True)
        )
        SELECT MAX(MAKE_DATE(year, month, day)) as date FROM dates
""").fetchone()[0].isoformat()
        return last_date


@app.get("/company/")
def company_metadata(isin: Annotated[str | None, Query(title="The ISIN of the company")] = None,
                     ducks3: DuckS3 = Depends(get_ducks3)):
    """Return metadata of companies with optional filtering by ISIN."""
    return ducks3.get_companies_metadata(isin=isin).to_dicts()


@app.get("/news")
async def news_daterange(
        date_from: Annotated[str | None, Query(title="The start date of the news")] = None,
        date_to: Annotated[str | None, Query(title="The end date of the news")] = None,
        isin: Annotated[str | None, Query(title="The ISIN of the company to fetch news")] = None,
        only_isin: Annotated[bool | None, Query(title="Fetch only news about WIG20 companies")] = None,
        ducks3: DuckS3 = Depends(get_ducks3)
):
    """Retrieves news data based on specified filters and returns it as a Parquet file."""
    data = ducks3.get_news(date_from=date_from, date_to=date_to, isin=isin, only_isin=only_isin)
    buffer = io.BytesIO()
    data.write_parquet(buffer)
    buffer.seek(0)
    return StreamingResponse(buffer,
                             media_type="application/octet-stream",
                             headers={"Content-Disposition": "attachment; filename=news.parquet"})


@app.get("/news/today")
async def today_news(ducks3: DuckS3 = Depends(get_ducks3)):
    """Retrieves today's news data and returns it as a json"""
    today = datetime.date.today().isoformat()
    return ducks3.get_news(date_from=today).to_dicts()


@app.get("/ohlc")
async def ohlc(ducks3: DuckS3 = Depends(get_ducks3), isin: str = None,
               date_from: str | None = None, date_to: str | None = None,
               mode: str = 'daily'):
    """
    Retrieves OHLC (Open, High, Low, Close) data and returns it as a Parquet file.

    This endpoint supports different aggregation modes for OHLC data, including daily and raw, highly-frequent data from
    gpw chart (minutely).
    The returned data is streamed as a Parquet file.

    Args:
        isin: Optional ISIN identifier for filtering OHLC data.
        date_from: Optional start date for minutely data filtering.
        date_to: Optional end date for minutely data filtering.
        mode: Aggregation mode, either 'daily' or 'minutely'.

    Returns:
        StreamingResponse containing the OHLC data in Parquet format.

    Raises:
        HTTPException: If the mode is not 'daily' or 'minutely'.
    """
    if mode == 'daily':
        data = ducks3.aggregate_ohlc_daily()

    elif mode == 'minutely':
        if not date_from:
            date_from = last_date_of_ohlc_data()
        data = ducks3.get_ohlc_minutely(isin=isin, date_from=date_from,
                                        date_to=date_to)
    else:
        raise HTTPException(status_code=404, detail="Mode not found")

    buffer = io.BytesIO()
    data.write_parquet(buffer)
    buffer.seek(0)

    return StreamingResponse(buffer,
                             media_type="application/octet-stream",
                             headers={"Content-Disposition": f"attachment; filename=ohlc{mode}.parquet"})


@app.get("/currencies")
async def currencies(curr_type: Annotated[str, Query(title="Type of currency",
                                                     enum=["mid_market", "bid_ask", "mid_market_rate_unpopular"])],
                     date_from: Annotated[str | None, Query(title="The start date")] = None,
                     date_to: Annotated[str | None, Query(title="The end date")] = None,
                     curr_code: Annotated[str | None, Query(title="Currency code (USD, CHF etc)")] = None,
                     ducks3: DuckS3 = Depends(get_ducks3)):
    currencies = ducks3.get_currencies(currency_type=curr_type, date_from=date_from, date_to=date_to,
                                       currency_code=curr_code)
    """Returns currency data for a given type and date range"""
    buffer = io.BytesIO()
    currencies.write_parquet(buffer)
    buffer.seek(0)

    return StreamingResponse(buffer,
                             media_type="application/octet-stream",
                             headers={"Content-Disposition": "attachment; filename=currencies.parquet"})


@app.get('/gold')
async def gold(date_from: Annotated[str | None, Query(title="The start date")] = None,
               date_to: Annotated[str | None, Query(title="The end date")] = None,
               ducks3: DuckS3 = Depends(get_ducks3)):
    """Return gold price data for a given date range, if no dates are specified return all data available in s3.
    Return parquet file."""
    data = ducks3.get_gold_prices(date_from=date_from, date_to=date_to)

    buffer = io.BytesIO()
    data.write_parquet(buffer)
    buffer.seek(0)
    return StreamingResponse(buffer,
                             media_type="application/octet-stream",
                             headers={"Content-Disposition": "attachment; filename=gold.parquet"})


@app.get('/llm_summary')
async def llm_summary(ducks3: DuckS3 = Depends(get_ducks3)):
    """Return the latest LLM summary of news, stock market, etc"""
    summary_date, summary = ducks3.get_llm_summary()
    return {"date": summary_date, "summary": summary}
