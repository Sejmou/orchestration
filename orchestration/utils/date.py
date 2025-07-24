from datetime import datetime, timedelta
import re
from typing import List


def date_isoformat(dt: datetime) -> str:
    """
    Returns the current date in ISO format without the time.
    """
    return dt.isoformat().split("T")[0]


def dt_to_fs_compatible_str(dt: datetime):
    return dt.strftime("%Y-%m-%d_%H-%M-%S")


def generate_daily_datetimes_between(start: datetime, end: datetime) -> List[datetime]:
    if start > end:
        raise ValueError("Start date must be before end date.")
    dates: List[datetime] = []
    # create datetimes for the beginning of the start and end date to avoid timezone issues
    start = datetime(start.year, start.month, start.day)
    end = datetime(end.year, end.month, end.day)
    current_datetime = start
    while current_datetime <= end:
        dates.append(current_datetime)
        current_datetime += timedelta(days=1)

    return dates


def generate_iso_date_strings(start: datetime, end: datetime) -> List[str]:
    dates = generate_daily_datetimes_between(start, end)
    date_strings = [date_isoformat(date) for date in dates]

    return date_strings


def yyyymmdd_file_path_to_datetime(file_path: str):
    """
    Extracts the date from a file path in the format "YYYYMMDD", returning a datetime object.
    Raises an exception if the date cannot be extracted.
    """
    match = re.search(r"\d{8}", file_path)
    if match:
        date_str = match.group(0)
        date_obj = datetime.strptime(date_str, "%Y%m%d")
        return date_obj
    else:
        raise Exception(f"Could not extract date from file path: {file_path}")


def iso_date_file_path_to_datetime(file_path: str):
    """
    Extracts the date from a file path in the format "YYYY-MM-DD", returning a datetime object.
    Raises an exception if the date cannot be extracted.
    """
    match = re.search(r"\d{4}-\d{2}-\d{2}", file_path)
    if match:
        date_str = match.group(0)
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj
    else:
        raise Exception(f"Could not extract date from file path: {file_path}")
