import os
import logging
import boto3
import hashlib
import json

from botocore.exceptions import ClientError
from datetime import timedelta, datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

def process_key(bucket, key_content):
  key = key_content['Key']
  last_modified_datetime = key_content['LastModified']
  verify_key_processed_success_exists_in_s3(bucket, key)
  return "success"

def process_keys(bucket, keys_contents):
  for key_content in keys_contents:
    process_key(bucket, key_content)

def read_s3_keys(bucket, prefix, s3_keys_listing_limit_per_call, start_after_key):
  kwargs = {'Bucket': bucket}
  kwargs['Prefix'] = prefix
  kwargs['MaxKeys'] = s3_keys_listing_limit_per_call
  if start_after_key:
    kwargs['StartAfter'] = start_after_key

  response = s3.list_objects_v2(**kwargs)
  contents = response.get('Contents', [])
  return contents

def verify_key_processed_success_exists_in_s3(bucket, key):
  data_response = s3.get_object(Bucket=bucket, Key=key)
  json_data_response = data_response['Body'].read().decode('utf-8')
  id = hashlib.md5(json_data_response.encode('utf-8')).hexdigest()
  processed_success_key = key.replace('json', '') + id + '.ods.processed.success'
  logger.info('Verifying if processed_success_key exists {}'.format(processed_success_key))
  try:
    s3.get_object(Bucket=bucket, Key=processed_success_key)
    return True
  except ClientError:
    return False

def lambda_handler(event, context):
  datetime_lambda_start = datetime.now()

  json_str = json.dumps(event)
  logger.info("Lambda received event {} ".format(json_str))
  json_object = json.loads(json_str)

  keys_contents = read_s3_keys("all-transactions", "us-east1", 1000, "")

  process_keys("all-transactions", keys_contents)

  current_datetime_lambda = datetime.now()
  lambda_exec_seconds = int((current_datetime_lambda - datetime_lambda_start).total_seconds())

  print(">>>>> " +str(lambda_exec_seconds))

  return "success"