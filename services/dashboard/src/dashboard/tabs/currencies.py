import streamlit as st
import polars as pl
import duckdb
from utils.plotting import plot_currency


def render(currencies: pl.DataFrame):

    select_currency = st.selectbox(
        label="Select currency",
        options=currencies.select('code').sort('code').unique(),
        format_func=lambda code:
        f"{currencies.filter(pl.col('code') == code).get_column('currency').item(1)} - {code}",
    )

    st.divider()
    currency = currencies.filter(pl.col('code') == select_currency)

    offsets = [
            '1W',
            '1M', '3M',
            '1Y', '5Y',
            'MAX']
    time_range = st.segmented_control(
        "Time Range",
        options=offsets,
        default="1M",
        label_visibility="collapsed",
        key='currency_time_range',
    )
    st.write(plot_currency(currency, time_range))

