import os
import logging
import boto3
import json

from datetime import timedelta, datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

def read_and_validate_environment_vars():
  bucket_name = os.environ["BUCKET_NAME"]
  if not bucket_name:
    raise Exception("bucket_name is not defined")
  historical_recovery_path = os.environ["HISTORICAL_RECOVERY_PATH"]
  if not historical_recovery_path:
    raise Exception("historical_recovery_path is not defined")
  max_s3_keys_per_call = os.environ["MAX_S3_KEYS_PER_CALL"]
  if not max_s3_keys_per_call:
    raise Exception("max_s3_keys_per_call is not defined")
  scan_date_start = os.environ["SCAN_DATE_START"]
  if not scan_date_start:
    raise Exception("scan_date_start is not defined")
  scan_date_end = os.environ["SCAN_DATE_END"]
  if not scan_date_end:
    raise Exception("scan_date_end is not defined")

  environment_vars = {
    "bucket": bucket_name,
    "historical_recovery_path": historical_recovery_path,
    "scan_date_start": scan_date_start,
    "scan_date_end": scan_date_end,
    "max_s3_keys_per_call": int(max_s3_keys_per_call)
  }
  return environment_vars

def lambda_handler(event, context):
  action_start_scan = "start-scan"
  action_scan_prefix = "scan-prefix"

  json_str = json.dumps(event)
  logger.info("Lambda received event {} ".format(json_str))
  json_object = json.loads(json_str)

  environment_vars = read_and_validate_environment_vars()
  bucket = environment_vars["bucket"]
  historical_recovery_path = environment_vars["historical_recovery_path"]
  scan_date_start = environment_vars["scan_date_start"]
  scan_date_end = environment_vars["scan_date_end"]
  scan_date_from = datetime.strptime(scan_date_start, '%m-%d-%Y')
  scan_date_to = datetime.strptime(scan_date_end, '%m-%d-%Y')
  max_s3_keys_per_call = environment_vars["max_s3_keys_per_call"]
  action = json_object['action']

  lambda_async_number = 0
  start_after = ""
  if action == action_start_scan:
    scan_date = scan_date_start
  elif action == action_scan_prefix:
    scan_date = json_object["scan_date"]
    start_after = json_object["lastVerifiedKey"]
    lambda_async_number = json_object["lambdaAsyncNumber"]

  prefix = historical_recovery_path + '/' + scan_date
  kwargs = {'Bucket': bucket}
  kwargs['Prefix'] = prefix
  kwargs['MaxKeys'] = max_s3_keys_per_call
  if start_after:
    kwargs['StartAfter'] = start_after

  resp = s3.list_objects_v2(**kwargs)
  contents = resp.get('Contents', [])
  len_contents = len(contents)
  logger.info('contents {}'.format(contents))
  last_verified_key = ""
  for content in contents:
    key = content['Key']
    last_verified_key = key
    data_response = s3.get_object(Bucket=bucket, Key=key)
    json_data_response = data_response['Body'].read().decode('utf-8')
    process_file(json_data_response)

  current_scan_datetime = datetime.strptime(scan_date, '%m-%d-%Y')
  if ((len_contents == 0 or len_contents < max_s3_keys_per_call) and current_scan_datetime == scan_date_to):
    logger.info('Finished data processing')
  else:
    if ((len_contents == 0 or len_contents < max_s3_keys_per_call) and current_scan_datetime < scan_date_to):
      next_scan_date = current_scan_datetime + timedelta(1)
      scan_date = next_scan_date.strftime('%m-%d-%Y')
      last_verified_key = ""
    payload = json.dumps({"action": action_scan_prefix,
                        "lambdaAsyncNumber": lambda_async_number + 1,
                        "scan_date": scan_date,
                        "lastVerifiedKey": last_verified_key
                        })
    lambda_client.invoke(
      FunctionName=context.function_name,
      InvocationType='Event',
      Payload=payload
    )

  return "success"

def process_file(file):
  logger.info('json_data_response {}'.format(file))
