from datetime import date
import pytest
import analytics.metrics as metrics
import polars as pl
from polars.testing import assert_frame_equal


@pytest.fixture
def sample_df():
    df = pl.read_json('ohlc_daily_data.json')
    return df


@pytest.fixture()
def sample_meta():
    meta = [
        {'name': 'PKOBP', 'company_isin': 'PLPKO0000016'},
        {'name': 'PZU', 'company_isin': 'PLPZU0000011'}
    ]

    return pl.DataFrame(meta, orient='row')


@pytest.fixture
def ohlc_daily_fixture() -> pl.DataFrame:
    """OHLC data fixture for testing"""
    data = [
        ('PLPKO0000016', date(2025, 11, 6), 100),
        ('PLPKO0000016', date(2025, 11, 7), 115),
        ('PLPZU0000011', date(2025, 11, 6), 80),
        ('PLPZU0000011', date(2025, 11, 7), 70)
    ]
    return pl.DataFrame(data, orient='row', schema={'isin': pl.String, 'date': pl.Date, 'close': pl.Float64})


def test_calculate_daily_stock_performance(ohlc_daily_fixture, sample_meta):
    expect_gainers = pl.DataFrame(
        {
            'isin': ['PLPKO0000016'],
            'date': [date(2025, 11, 7)],
            'close': [115],
            'prev_close': [100],
            'change': [15.0],
            'type': ['gainer']

        }, schema={'isin': pl.String, 'date': pl.Date, 'close': pl.Float64, 'prev_close': pl.Float64,
                   'change': pl.Float64, 'type': pl.String}
    )
    expect_losers = pl.DataFrame(
        {
            'isin': ['PLPZU0000011'],
            'date': [date(2025, 11, 7)],
            'close': [70],
            'prev_close': [80],
            'change': [-12.5],
            'type': 'loser'
        }, schema={'isin': pl.String, 'date': pl.Date, 'close': pl.Float64, 'prev_close': pl.Float64,
                   'change': pl.Float64, 'type': pl.String},
    )

    gainers, losers = metrics.calculate_daily_stock_performance(ohlc_daily_fixture, sample_meta)
    key_columns = ['isin', 'date', 'close', 'prev_close', 'change', 'type']
    gainers = gainers.select(key_columns)
    losers = losers.select(key_columns)
    assert_frame_equal(gainers, expect_gainers)
    assert_frame_equal(losers, expect_losers)


@pytest.fixture
def gold_changes_fixture() -> pl.DataFrame:
    """Fixture for testing gold changes"""
    data = pl.DataFrame({
        'date': [date(2025, 11, 5), date(2025, 11, 6),
                 date(2025, 11, 7)],
        'price': [400.0, 420.0, 378.0],
    })
    return data


def test_calculate_gold_price_changes(gold_changes_fixture):
    expected = pl.DataFrame([
        (date(2025, 11, 7), 378.0, 420.0, -10.0),
        (date(2025, 11, 6), 420.0, 400.0, 5.0),
        (date(2025, 11, 5), 400.0, None, None)
    ], orient="row",
        schema={'date': pl.Date, 'price': pl.Float64, 'prev_price': pl.Float64, 'change': pl.Float64})
    gold_changes = (metrics.calculate_gold_changes(gold_changes_fixture, limit=None).
                    select(pl.exclude('is_rise')))
    assert_frame_equal(gold_changes, expected)


@pytest.fixture
def currencies_changes_fixture() -> pl.DataFrame:
    data = pl.DataFrame(
        [
            (date(2020, 11, 6), 'USD', 4.0),
            (date(2020, 11, 7), 'USD', 4.4),
            (date(2020, 11, 6), 'CHF', 5.0),
            (date(2020, 11, 7), 'CHF', 4.5),
    ],
    schema = {'effective_date': pl.Date, 'code': pl.String, 'mid': pl.Float64},
        orient="row"
    )
    return data

def test_calculate_currencies_changes(currencies_changes_fixture):
    expected = pl.DataFrame(
        [
            (date(2020, 11, 6), 'CHF', 5.0, None, None),
            (date(2020, 11, 7), 'CHF', 4.5, 5, -10.0),
            (date(2020, 11, 6), 'USD', 4.0, None, None),
            (date(2020, 11, 7), 'USD', 4.4, 4, 10.0)
        ],
        schema={'effective_date': pl.Date, 'code': pl.String, 'mid': pl.Float64, 'prev_mid': pl.Float64,
                'change': pl.Float64},
        orient="row"
    ).sort(by=['effective_date', 'code'], descending=[True, False])

    currencies_changes = metrics.calculate_currencies_changes(currencies_changes_fixture)
    currencies_changes = currencies_changes.select('effective_date', 'code', 'mid', 'prev_mid', 'change')
    assert_frame_equal(currencies_changes, expected)


