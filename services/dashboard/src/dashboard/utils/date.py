from datetime import date, timedelta

def get_week_start_end_dates():
    """
    Retrieves monday and friday date of the current week.

    Returns:
        tuple: A tuple containing two string values in ISO format:
            - The first string represents the Monday date of the current week
            - The second string represents the Friday date of the current week
    """
    current_week_day = date.today().weekday()
    monday_date = date.today() - timedelta(days=current_week_day)
    friday_date = monday_date + timedelta(days=4)
    monday_date = monday_date.isoformat()
    friday_date = friday_date.isoformat()
    return monday_date, friday_date