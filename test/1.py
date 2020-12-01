from datetime import timedelta, datetime

scan_date_start = "11-01-2020"
scan_date_end = "11-01-2020"

scan_date_from = datetime.strptime(scan_date_start, '%m-%d-%Y')
scan_date_to = datetime.strptime(scan_date_end, '%m-%d-%Y')

scan_dates = [scan_date_start]
temp_date_time = scan_date_from
if scan_date_start != scan_date_end:
  while True:
    next_date_time = temp_date_time + timedelta(1)
    temp_date_time = next_date_time
    scan_dates.append(next_date_time.strftime('%m-%d-%Y'))
    if next_date_time == scan_date_to:
      break


print(scan_dates)
scan_dates_len = len(scan_dates)
for iteartion in range(0, scan_dates_len):
  print(scan_dates[iteartion])


text = "hist-rec/11-01-2020/test/1/1.json"

if text.startswith('hist-rec/11-01-2020'):
  print("yes")

len_contents = 0
max_s3_keys_per_call = 1000
scan_date = '11-11-2020'
scan_date_end = '11-11-2020'

if ((len_contents == 0 or len_contents < max_s3_keys_per_call) and scan_date == scan_date_end):
  print("xxxx")