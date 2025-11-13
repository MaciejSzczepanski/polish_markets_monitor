from datetime import date

import pytest
from data_access.validators import validate_isin, parse_date


@pytest.mark.parametrize("isin, expected",
                         [("PLPKN0000018", None),
                          ("LU2237380790", None),
                          ("110000000018", ValueError),
                          ("ABC11", ValueError)]
                         )
def test_validate_isin(isin, expected):
    if expected is None:
        assert validate_isin(isin) is None
    else:
        with pytest.raises(expected):
            validate_isin(isin)

@pytest.mark.parametrize("date_string, expected",
                         [("2023-01-01", date(2023, 1, 1)),
                          ("20 czerwca 2025", ValueError),
                          ("20-12-2025", ValueError),
                          ("2025/11/10", ValueError)
                          ])
def test_parse_date(date_string, expected):
    if isinstance(expected, date):
        assert parse_date(date_string) == expected
    else:
        with pytest.raises(expected):
            parse_date(date_string)