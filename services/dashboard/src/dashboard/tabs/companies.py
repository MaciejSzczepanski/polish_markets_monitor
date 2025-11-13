import streamlit as st
import polars as pl
from components.news_list import news_section
from dashboard.utils.plotting import plot_ohlc
from utils.plotting import plot_volume


def render(companies_meta: pl.DataFrame,
           ohlc_daily: pl.DataFrame,
           ohlc_minutely_today: pl.DataFrame,
           news: pl.DataFrame,
           ) -> None:
    tickers = companies_meta.sort('ticker').get_column('ticker').to_list()
    selected_ticker_from_overview = st.session_state.get('selected_ticker', None)
    if selected_ticker_from_overview:
        selected_ticker_from_overview = tickers.index(selected_ticker_from_overview)
    else:
        selected_ticker_from_overview = 0
    selected_ticker = st.selectbox(
        label="Select company",
        options=tickers,
        format_func=lambda ticker:
        f"{(companies_meta.filter(pl.col('ticker') == ticker).get_column('name').item())} ({ticker})",
        index=selected_ticker_from_overview,
        key="ticker_dropdown"
    )

    st.divider()
    company_meta = companies_meta.filter(pl.col('ticker') == selected_ticker).to_dicts()[0]

    st.subheader(f"{company_meta['full_name']} {company_meta['ticker']}")
    st.write(f'''listed since: {company_meta["listed_since"]}''')
    st.write(f'{company_meta["description"]}')
    st.divider()

    offsets = {
        '1D': '-1d',
        '1W': '-1w',
        '1M': '-1mo',
        '3M': '-3mo',
        '1Y': '-1y',
        '5Y': '-5y',
        'MAX': None
    }
    col1, col2, col3 = st.columns([1, 3, 1])
    with col2:
        time_range = st.segmented_control(
            "Time Range",
            options=offsets.keys(),
            default="1D",
            label_visibility="collapsed"
        )
    if time_range == '1D':
        ohlc_data = ohlc_minutely_today.filter(pl.col('isin') == company_meta['company_isin'])
    else:
        ohlc_data = ohlc_daily.filter(pl.col('isin') == company_meta['company_isin'])

    st.write(plot_ohlc(ohlc_data, selected_ticker, time_range))

    st.write(plot_volume(ohlc_data, selected_ticker, time_range))

    news_section(news, isin=company_meta['company_isin'])
