import os
import logging
import boto3
import json

from datetime import timedelta, datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')
s3_client = boto3.resource('s3')

def read_and_validate_environment_vars():
  bucket_name = os.environ["BUCKET_NAME"]
  if not bucket_name:
    raise Exception("bucket_name is not defined")
  historical_recovery_path = os.environ["HISTORICAL_RECOVERY_PATH"]
  if not historical_recovery_path:
    raise Exception("historical_recovery_path is not defined")

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
    "scan_date_end": scan_date_end
  }
  return environment_vars

def lambda_handler(event, context):
  action_scan = "scan-historical-recovery-path"

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
  action = json_object['action']

  if action == action_scan:
    scan_dates = [scan_date_start]
    temp_date_time = scan_date_from
    while True:
      next_date_time = temp_date_time + timedelta(1)
      temp_date_time = next_date_time
      scan_dates.append(next_date_time.strftime('%m-%d-%Y'))
      if next_date_time == scan_date_to:
        break

      scan_dates_len = len(scan_dates)
      for iteartion in range(0, scan_dates_len):
        scan_date = scan_dates[iteartion]
        prefix = historical_recovery_path + '/' + scan_date
        logger.info('Prefix {}'.format(prefix))
        bucket = s3_client.Bucket(bucket)
        logger.info('Test >>>> ')
        for obj in bucket.objects.filter(Prefix=prefix):
         file_key = obj.key
         logger.info('Fetching the key {}'.format(file_key))
         data = s3.get_object(Bucket=bucket, Key=file_key)
         contents = data['Body'].read().decode()
         logger.info('Fetching the content inside file {}'.format(contents))
         response_file_key = contents
         logger.info('Fetching response_file_key {}'.format(response_file_key))
  return "success"
