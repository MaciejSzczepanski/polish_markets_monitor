import datetime
import re

def validate_isin(isin: str) -> None:
    """
    Validates that a string conforms to the ISIN (International Securities Identification Number) format.

    An ISIN consists of 12 characters: the first two are letters representing the country code,
    and the remaining ten are alphanumeric characters. This function checks that the input string
    matches this exact pattern.

    Args:
        isin: A string representing an ISIN to validate

    Raises:
        ValueError: If the ISIN does not match the required format of two uppercase letters
            followed by ten uppercase letters or digits
    """
    if isin is None:
        return

    if not re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', isin):
        raise ValueError("Invalid isin")


def parse_date(date: str) -> datetime.date | None:
    """
    Parses a date string in YYYY-MM-DD format into a datetime.date object.

    Args:
        date: A string representing a date in YYYY-MM-DD format.

    Returns:
        A datetime.date object if the input string is valid, or None if the input is None.

    Raises:
        ValueError: If the input string is not in the correct format (YYYY-MM-DD).
    """
    if date is None:
        return None

    if not re.match(r'^\d{4}-\d{2}-\d{2}$', date):
        raise ValueError("Invalid date")

    return datetime.datetime.strptime(date, "%Y-%m-%d").date()


