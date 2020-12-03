import os
import logging
import boto3
import hashlib
import json

from botocore.exceptions import ClientError
from collections import namedtuple
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

Rule = namedtuple('Rule', ['has_min', 'has_max'])
last_modified_rules = {
  Rule(has_min=True, has_max=True):
    lambda min_date, date, max_date: min_date <= date <= max_date,
  Rule(has_min=True, has_max=False):
    lambda min_date, date, max_date: min_date <= date,
  Rule(has_min=False, has_max=True):
    lambda min_date, date, max_date: date <= max_date,
  Rule(has_min=False, has_max=False):
    lambda min_date, date, max_date: True,
}

def read_and_validate_environment_vars():
  historical_recovery_path = os.environ["HISTORICAL_RECOVERY_PATH"]
  if not historical_recovery_path:
    raise Exception("historical_recovery_path is not defined")
  suffixes = os.environ["SUFFIXES"]
  if not suffixes:
    raise Exception("suffixes are not defined")
  last_modified_start = os.environ["LAST_MODIFIED_START"]
  if not last_modified_start:
    raise Exception("last_modified_start is not defined")
  last_modified_end = os.environ["LAST_MODIFIED_END"]
  if not last_modified_end:
    raise Exception("last_modified_end is not defined")
  last_modified_start_datetime = datetime.strptime(last_modified_start, '%m/%d/%Y %H:%M:%S%z')
  last_modified_end_datetime = datetime.strptime(last_modified_end, '%m/%d/%Y %H:%M:%S%z')

  if last_modified_start_datetime and last_modified_end_datetime and last_modified_end_datetime < last_modified_start_datetime:
    raise ValueError("last_modified_end_datetime {} must be greater than last_modified_start_datetime {}"
                     .format(last_modified_end_datetime, last_modified_start_datetime))

  environment_vars = {
    "historical_recovery_path": historical_recovery_path,
    "suffixes": tuple(set(suffixes.split(","))),
    "last_modified_start_datetime": last_modified_start_datetime,
    "last_modified_end_datetime": last_modified_end_datetime
  }
  return environment_vars

def get_not_processed_success_s3_keys(bucket, prefix, suffixes, verify_from_key, verify_to_key,
    last_modified_start_datetime, last_modified_end_datetime):
  result_not_processed_success_keys = []
  last_modified_rule = last_modified_rules[bool(last_modified_start_datetime), bool(last_modified_end_datetime)]
  kwargs = {'Bucket': bucket}
  kwargs['Prefix'] = prefix
  if verify_from_key:
    kwargs['StartAfter'] = verify_from_key
  stop_verification = False

  while True:
    last_verified_key = ""
    resp = s3.list_objects_v2(**kwargs)
    contents = resp.get('Contents', [])
    if len(contents) == 0:
      logger.info("Stop verification. No keys to process returned from S3.")
      break
    for content in contents:
      key = content['Key']
      logger.info('Verifying key {}'.format(key))
      last_verified_key = key
      last_modified_datetime = content['LastModified']
      if (key.endswith(suffixes) and
          last_modified_rule(last_modified_start_datetime, last_modified_datetime, last_modified_end_datetime) and
          key_is_not_processed_success(bucket, key)
      ):
        result_not_processed_success_keys.append(content)
      if key == verify_to_key:
        stop_verification = True
        logger.info("Stop verification. Reached last key in verification range keys.")
        break
    if stop_verification:
      break
    kwargs['StartAfter'] = last_verified_key
  return result_not_processed_success_keys

def key_is_not_processed_success(bucket, key):
  data_response = s3.get_object(Bucket=bucket, Key=key)
  json_data_response = data_response['Body'].read().decode('utf-8')
  id = hashlib.md5(json_data_response.encode('utf-8')).hexdigest()
  processed_success_key = key.replace('json', id + '.ods.processed.success')
  try:
    #check if success marker file exists
    s3.get_object(Bucket=bucket, Key=processed_success_key)
    #exists, file processed
    logger.info('Exists processed success key {} for file {} in bucket {} '
                .format(processed_success_key, key, bucket))
    return False
  except ClientError:
    #does not exist, file not processed
    logger.info('Does not exist processed success key {} for file {} in bucket {} '
                .format(processed_success_key, key, bucket))
    return True

def lambda_handler(event, context):
  json_event = json.dumps(event)
  logger.info("Lambda received event {} ".format(json_event))

  environment_vars = read_and_validate_environment_vars()
  historical_recovery_path = environment_vars["historical_recovery_path"]
  suffixes = environment_vars["suffixes"]
  last_modified_start_datetime = environment_vars["last_modified_start_datetime"]
  last_modified_end_datetime = environment_vars["last_modified_end_datetime"]

  for record in event['Records']:
    body = json.loads(record["body"])
    bucket = body["bucket"]
    prefix = body["prefix"]
    id = body["id"]
    verify_from_key = body["verifyFromKey"]
    verify_to_key = body["verifyToKey"]

    # validate if not empty body data

    keys = get_not_processed_success_s3_keys(bucket, prefix, suffixes, verify_from_key, verify_to_key,
                                             last_modified_start_datetime, last_modified_end_datetime)
    if len(keys) > 0:
      logger.info("Started to save not processed success keys to historical recovery path")
    else:
      logger.info("Not processed success keys have not been found. Nothing to save to historical recovery path")

    for content in keys:
      response_file_key = content["Key"]
      last_modified_datetime = content['LastModified']
      last_modified_date = last_modified_datetime.strftime("%m-%d-%Y")
      file_name = response_file_key.rsplit('/', 1)[-2] #transactionID
      historical_recovery_key = historical_recovery_path + '/' + last_modified_date + '/' + file_name + '.txt'
      logger.info('Generate historical recovery file {} in bucket {}'.format(historical_recovery_key, bucket))
      s3.put_object(Body=response_file_key.encode(), Bucket=bucket, Key=historical_recovery_key)

  s3.put_object(Body=id.encode(), Bucket=bucket, Key=historical_recovery_path + "/" + id + "/" + id + ".txt")