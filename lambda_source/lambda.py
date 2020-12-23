import os
import logging
import boto3
import hashlib
import json

from concurrent.futures import ThreadPoolExecutor
from time import sleep
from datetime import timedelta, datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

def process_key(key_content):
  file_key = key_content['Key']
  last_modified_datetime = key_content['LastModified']
  logger.info('Verifying key {}'.format(file_key))
  return "success"

def process_keys(keys_contents, executor):
  futures = []
  for key_content in keys_contents:
    future = executor.submit(process_key, (key_content))
    futures.append(future)

  all_threads_done = False
  while all_threads_done == False:
    for future in futures:
      if future.done() == False:
        break
      if future == futures[len(futures) - 1]:
        all_threads_done = True
  print("all_threads_done: " + str(all_threads_done))

def read_s3_keys(bucket, prefix, s3_keys_listing_limit_per_call, start_after_key):
  kwargs = {'Bucket': bucket}
  kwargs['Prefix'] = prefix
  kwargs['MaxKeys'] = s3_keys_listing_limit_per_call
  if start_after_key:
    kwargs['StartAfter'] = start_after_key

  response = s3.list_objects_v2(**kwargs)
  contents = response.get('Contents', [])
  return contents

def lambda_handler(event, context):
  datetime_lambda_start = datetime.now()

  json_str = json.dumps(event)
  logger.info("Lambda received event {} ".format(json_str))
  json_object = json.loads(json_str)

  executor = ThreadPoolExecutor(1000)

  keys_contents = read_s3_keys("all-transactions", "us-east1", 1000, "")

  process_keys(keys_contents, executor)

  current_datetime_lambda = datetime.now()
  lambda_exec_seconds = int((current_datetime_lambda - datetime_lambda_start).total_seconds())

  print(lambda_exec_seconds)

  return "success"