from datetime import datetime, timedelta

def dag_start_date(start_date=None, reduce_days=None):
    if start_date is None or not isinstance(start_date, datetime):
        start_date = datetime.now()
    if reduce_days is None or not isinstance(reduce_days, int):
        reduce_days = 15
    delta = timedelta(days=-reduce_days)
    date_time = start_date + delta
    year = int(date_time.strftime('%Y'))
    month = int(date_time.strftime('%m'))
    day = int(date_time.strftime('%d'))
    return datetime(year=year, month=month, day=day, hour=0, minute=0, second=0)
