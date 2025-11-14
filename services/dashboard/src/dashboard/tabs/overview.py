import streamlit as st
from analytics.metrics import calculate_gold_changes, calculate_currencies_changes, \
    calculate_daily_stock_performance
import duckdb
import polars as pl


def update_ohlc_daily_aggregated(ohlc_daily: pl.DataFrame, ohlc_today_minutely: pl.DataFrame):
    # ensuring the latest date/'live' data is added to the ohlc_daily dataframe which contains aggregated data
    with duckdb.connect(database=":memory:") as conn:
        conn.sql("SET TimeZone = 'Europe/Warsaw'")
        ohlc_today_aggregated = duckdb.sql("""SELECT date::DATE               as date,
                                                     isin,
                                                     ARG_MIN(price, datetime) as open,
                                                     ARG_MAX(price, datetime) as close,
                                                     MIN(price)               as low,
                                                     MAX(price)               as high,
                                                     SUM(volume)              as volume
                                              FROM ohlc_today_minutely
                                              GROUP BY isin, date::DATE""").pl()
    last_date_ohlc_minutely = ohlc_today_aggregated.select(pl.col('date').dt.date()).head(1).item()

    ohlc_daily = pl.concat(
        [ohlc_daily.filter(pl.col('date').dt.date() != last_date_ohlc_minutely), ohlc_today_aggregated],
        how='vertical_relaxed')

    return ohlc_daily


def render(ohlc_daily: pl.DataFrame,
           ohlc_today_minutely: pl.DataFrame,
           companies_meta: pl.DataFrame,
           popular_currencies: pl.DataFrame,
           gold_prices: pl.DataFrame,
           llm_summary: dict):
    gold_prices = calculate_gold_changes(gold_prices)

    popular_currencies = (calculate_currencies_changes(popular_currencies).rename({'mid': 'price'})
                          .sort('effective_date')
                          )
    n_currencies = popular_currencies.unique(subset='code').shape[0]
    popular_currencies = popular_currencies.tail(n_currencies).to_dicts()

    #               )
    st.write(llm_summary['summary'])
    st.divider()
    st.subheader("🏆 Top Movers Today")
    col1, col2 = st.columns(2)

    ohlc_daily = update_ohlc_daily_aggregated(ohlc_daily, ohlc_today_minutely)
    gainers, losers = calculate_daily_stock_performance(ohlc_daily, companies_meta)

    with col1:
        st.markdown("### 📈 Gainers")
        for i, company in enumerate(gainers.to_dicts()):
            # Green color + fire emoji for top gainer
            emoji = "🔥" if i == 0 else ""

            # Clickable metric
            if st.button(
                    f"{company['ticker']}: +{company['change']:.2f}% {emoji}",
                    key=f"gainer_{company['isin']}",
                    type="secondary",
                    use_container_width=True
            ):
                # Jump to Stocks tab with this ticker pre-selected
                st.session_state['selected_ticker'] = company['ticker']
                st.session_state['requested_tab'] = "📈 Companies"
                st.rerun()

            if i > 1: break

    with col2:
        st.markdown("### 📉 Losers")
        for i, company in enumerate(losers.to_dicts()):
            emoji = "❄️" if i == 0 else ""

            if st.button(
                    f"{company['ticker']}: {company['change']:.2f}% {emoji}",
                    key=f"loser_{company['isin']}",
                    type="secondary",
                    use_container_width=True
            ):
                st.session_state['selected_ticker'] = company['ticker']
                st.session_state['requested_tab'] = "📈 Companies"
                st.rerun()

            if i > 1: break

    num_cols = 4
    num_rows = 5

    st.divider()

    st.subheader("All Stocks(WIG20)")
    companies_value_change = pl.concat([gainers, losers])
    for row in range(num_rows):
        cols = st.columns(num_cols)

        for col_idx, col in enumerate(cols):
            company_df_index = (row * num_cols) + col_idx
            company_ticker, company_change = (companies_value_change.select('ticker', 'change').
                                              row(company_df_index))
            with col:
                # Emoji based on change
                if company_change > 0:
                    emoji = "🟢"
                    color = "green"
                elif company_change < 0:
                    emoji = "🔴"
                    color = "red"
                else:
                    emoji = "⚪"
                    color = "gray"

                button_label = f"{emoji} **{company_ticker}**\n{company_change:+.2f}%"

                if st.button(
                        button_label,
                        key=f"grid_{company_ticker}",
                        use_container_width=True,
                        type="secondary"
                ):
                    # Store selected ticker and switch to Stocks tab
                    st.session_state['selected_ticker'] = companies_value_change[company_df_index]['ticker'].item()
                    st.session_state['requested_tab'] = "📈 Companies"
                    st.rerun()

    st.divider()

    currencies_emoji = {'EUR': '🇪🇺', 'USD': '🇺🇸', 'CHF': '🇨🇭',
                        'gold': '🪙'
                        }

    cols = st.columns(4)

    generate_currency_gold_html = lambda currency: st.html(f"""
<div class='centered-column'>
            {emoji} {currency['code']}/PLN<br>
            <strong>{currency['price']:.4f} PLN</strong><br>
            <span style="color: {'#28a745' if currency['is_rise'] else '#dc3545'}">{'▲' if currency['change'] > 0 else '▼'} 
            {currency['change']:+.2f}%</span><br>
            {currency['effective_date']}<br>
            </div>
            """)

    for col_idx, currency in enumerate(popular_currencies):
        emoji = currencies_emoji.get(currency['code'])
        with cols[col_idx]:
            generate_currency_gold_html(currency)

    with cols[-1]:
        gold_prices = gold_prices.to_dicts()[0]
        gold_prices['code'] = 'GOLD'
        gold_prices['effective_date'] = gold_prices.pop('date')
        emoji = currencies_emoji.get('gold')
        generate_currency_gold_html(gold_prices)
