import streamlit as st
from tabs import overview, companies, news, currencies
from utils.data_loader import load_ohlc_daily, load_today_ohlc_minutely, load_companies_meta, \
    load_all_news, load_currencies, load_gold_prices, load_llm_summary
from datetime import date

api_url = "http://stock-api:8000"
today = date.today()

st.markdown("""
    <style>
    h1, h2, h3 {
        text-align: center;
    }
    </style>
    """, unsafe_allow_html=True)


companies_meta = load_companies_meta()
ohlc_daily = load_ohlc_daily(today)
ohlc_today_minutely = load_today_ohlc_minutely()
all_news = load_all_news()
currencies_all  = load_currencies()
popular_currencies = load_currencies(curr_type='mid_market_rate',
                                     currencies_list= ['USD', 'EUR', 'CHF'])
gold_prices = load_gold_prices()
llm_summary = load_llm_summary()

st.markdown("""
<style>
.centered-column {
    text-align: center;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""# Polish Markets Monitor 
""")

st.divider()

if 'active_tab' not in st.session_state:
    st.session_state['active_tab'] = "📊 Overview"

if st.session_state.get('requested_tab'):
    st.session_state['active_tab'] = st.session_state.requested_tab
    st.session_state.requested_tab = None

selected_tab = st.segmented_control(
    "Navigation",
    options= [ "📊 Overview",  # Summary + Top Movers + All Stocks grid
    "📈 Companies",  # Dropdown → details per company
    "💱 Currencies",  # Dropdown → details per currency
    "📰 News"  # All news chronological + filters
               ],
    key='active_tab',
    label_visibility="collapsed"
)

st.divider()


if selected_tab == "📊 Overview":
    overview.render(ohlc_daily, ohlc_today_minutely, companies_meta, popular_currencies, gold_prices, llm_summary)
elif selected_tab == "📈 Companies":
    companies.render(companies_meta, ohlc_daily, ohlc_today_minutely, all_news)
elif selected_tab == "💱 Currencies":
    currencies.render(currencies_all)
else:  # News
    news.render(all_news)