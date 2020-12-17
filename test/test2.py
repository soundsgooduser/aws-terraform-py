from datetime import timedelta, datetime

def create_dates(start_datetime, end_datetime):
  print("enter")
  start_date = start_datetime.strftime('%m-%d-%Y')
  result = {start_date: 2}
  temp_date_time = start_datetime
  if start_datetime == end_datetime:
    return result
  while True:
    next_date_time = temp_date_time + timedelta(1)
    temp_date_time = next_date_time
    next_date = next_date_time.strftime('%m-%d-%Y')
    result[next_date] = 0
    if next_date_time == end_datetime:
      break
  return result

def update_and_get_total_found_not_processed_keys_per_day(key_last_modified_datetime, dates):
  key_last_modified_date = key_last_modified_datetime.strftime('%m-%d-%Y')
  total_not_processed_keys = dates.get(key_last_modified_date)
  total_not_processed_keys = total_not_processed_keys + 1
  dates[key_last_modified_date] = total_not_processed_keys
  return total_not_processed_keys



print("enter 1")
scan_date_from = datetime.strptime('11-07-2020', '%m-%d-%Y')
scan_date_to = datetime.strptime('11-14-2020', '%m-%d-%Y')
key_last_modified_datetime = datetime.strptime('11-09-2020', '%m-%d-%Y')
print(scan_date_from)
dates = create_dates(scan_date_from, scan_date_to)
print(dates.get('11-07-2020'))

total_not_processed_keys = update_and_get_total_found_not_processed_keys_per_day(key_last_modified_datetime, dates)
print(dates)