import streamlit as st
from plotly.subplots import make_subplots
import plotly.graph_objs as go
import polars as pl


def _filter_date_by_offset(df: pl.DataFrame, offset: str) -> pl.DataFrame:
    """Filter dataframe by date offset"""
    last_date = df.select(pl.col('date').max()).item()
    df = df.filter(pl.col('date') >= pl.lit(last_date).dt.offset_by(offset))
    return df


@st.fragment
def plot_volume(df: pl.DataFrame, ticker: str, date_range: str) -> go.Figure:
    """Create volume bar chart"""
    offsets = {
        '1D': '-1d',
        '1W': '-1w',
        '1M': '-1mo',
        '3M': '-3mo',
        '1Y': '-1y',
        '5Y': '-5y',
        'MAX': None
    }
    offset = offsets.get(date_range, None)
    if offset == '-1d':
        df = (
            df
            .sort('datetime')
            .group_by_dynamic('datetime', every='1m')  # polishing data for 1-minute OHLC
            .agg([
                pl.col('volume').sum()
            ]).rename({'datetime': 'date'})
        )
    if offset:
        df = _filter_date_by_offset(df, offset)

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df['date'],
            y=df['volume'],
            name='Volume',
        )
    )

    fig.update_layout(
        title=f'Volume',
        xaxis_title='Date',
        yaxis_title='Volume',
        template='plotly_dark',
        height=400,
        hovermode='x unified',
    )

    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["sat", "mon"]),
        ]
    )

    return fig


@st.fragment
def plot_ohlc(df: pl.DataFrame, ticker: str, date_range: str) -> go.Figure:
    """Create interactive OHLC charts"""
    offsets = {
        '1D': '-1d',
        '1W': '-1w',
        '1M': '-1mo',
        '3M': '-3mo',
        '1Y': '-1y',
        '5Y': '-5y',
        'MAX': None
    }
    offset = offsets[date_range]

    if date_range == '1D':
        df = (
            df
            .sort('datetime')
            .group_by_dynamic('datetime', every='1m')  # polishing data for 1-minute OHLC
            .agg([
                pl.col('open').first(),
                pl.col('high').max(),
                pl.col('low').min(),
                pl.col('close').last(),
            ]).rename({'datetime': 'date'})
        )
    elif offset:
        df = _filter_date_by_offset(df, offset)

    fig = make_subplots()

    # OHLC Candlestick
    fig.add_trace(go.Candlestick(
        x=df['date'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='OHLC',
        increasing_line_color='#26A69A',  # green
        decreasing_line_color='#EF5350'  # red
    ))

    fig.update_layout(
        title=f'Price movement',
        xaxis_title='Date',
        yaxis_title='Price (PLN)',
        template='plotly_dark',  # dark theme
        height=500,
        hovermode='x unified',
        xaxis_rangeslider_visible=False,  # hide range slider
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )

    return fig


@st.fragment
def plot_currency(df: pl.DataFrame, date_range: str) -> go.Figure:
    """Create currency price change for selected date range"""
    df = df.rename({'effective_date': 'date'})
    offsets = {
        '1D': '-1d',
        '1W': '-1w',
        '1M': '-1mo',
        '3M': '-3mo',
        '1Y': '-1y',
        '5Y': '-5y',
        'MAX': None
    }

    offset = offsets.get(date_range, None)
    if offset:
        df = _filter_date_by_offset(df, offset)

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df['date'],
            y=df['mid'],
            name='a',
        )
    )

    fig.update_layout(
        xaxis_title='Date',
        yaxis_title='Price(PLN)',
        template='plotly_dark',
        height=400,
        hovermode='x unified',
    )

    return fig
