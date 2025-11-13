import streamlit as st
import polars as pl
from datetime import date
from components.button_load_more import load_more_button


def render(all_news: pl.DataFrame):
    inital_limit = 2  # days of news to load at 1st time or add after clicking "Load more"
    state_key = 'news_offset_tab'
    if state_key not in st.session_state:
        st.session_state[state_key] = 0
    news_limit = st.session_state[state_key] + inital_limit

    today = date.today()
    news_grouped_by_date = (all_news.sort('date', descending=True)
                            .group_by(pl.col('date').dt.to_string('%Y-%m-%d'), maintain_order=True)
                            .agg(news=pl.struct(pl.all()))
                            .with_columns(pl.when(pl.col('date') == today.isoformat()).
                                          then(pl.lit("Today")))
                            )
    len_news = news_grouped_by_date.shape[0]

    news_grouped_by_date = news_grouped_by_date.head(news_limit).to_dicts()

    for day in news_grouped_by_date:
        st.markdown(f"#### {day['date']}")
        for item in day['news']:
            st.markdown(f"**[{item['title']}]({item['link']})** ")

    load_more_button(len_news, state_key, inital_limit)