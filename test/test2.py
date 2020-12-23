from concurrent.futures import ThreadPoolExecutor
from time import sleep
from datetime import timedelta, datetime

def process_key(key):
  sleep(1)
  print("process key: " + key)
  return "processed key: " + key

def process_keys(keys):
  futures = []
  for key in keys:
    future = executor.submit(process_key, (key))
    futures.append(future)

  all_threads_done = False
  while all_threads_done == False:
    for future in futures:
      if future.done() == False:
        break
      if future == futures[len(futures) - 1]:
        all_threads_done = True
  print("all_threads_done: " + str(all_threads_done))

datetime_lambda_start = datetime.now()

executor = ThreadPoolExecutor(1000)
keys = ["1", "2", "3","4", "5", "6","7", "8", "9","10", "11", "12","13", "14", "15","16", "17", "18","19", "20"]

process_keys(keys)

current_datetime_lambda = datetime.now()
lambda_exec_seconds = int((current_datetime_lambda - datetime_lambda_start).total_seconds())

print(lambda_exec_seconds)


