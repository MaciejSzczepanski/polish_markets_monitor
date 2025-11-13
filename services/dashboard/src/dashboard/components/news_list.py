import streamlit as st
import polars as pl
from .button_load_more import load_more_button

@st.fragment
def news_section(news: pl.DataFrame, isin: str = None, initial_limit=5):
    """News section component for displaying company news with loading more functionality"""

    state_key = f"news_offset_{isin or 'all'}"
    if isin:
        news = news.filter(pl.col('company_isins').list.contains(isin))

    len_news = news.shape[0]

    if len_news == 0:
        return

    st.subheader("News")

    if state_key not in st.session_state:
        st.session_state[state_key] = 0

    news = news.head(st.session_state[state_key] + initial_limit)
    news = (news.with_columns(date_to_display = pl.col('date').dt.strftime('%Y-%m-%d'))
            .sort('date', descending=True))
    for item in news.iter_rows(named=True):
        st.markdown(f"**{item['date_to_display']}** [{item['title']}]({item['link']})")

    load_more_button(len_news, state_key, initial_limit)