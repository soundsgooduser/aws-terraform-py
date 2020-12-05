import os
import logging
import boto3
import hashlib
import json
import time

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
  save_result_to_s3 = os.environ["SAVE_RESULT_TO_S3"]
  fetch_max_s3_keys_per_s3_listing_call = os.environ["FETCH_MAX_S3_KEYS_PER_S3_LISTING_CALL"]
  if not fetch_max_s3_keys_per_s3_listing_call:
    raise Exception("fetch_max_s3_keys_per_s3_listing_call is not defined")
  if not save_result_to_s3:
    raise Exception("save_result_to_s3 are not defined")
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

  historical_recovery_path_metadata = os.environ["HISTORICAL_RECOVERY_PATH_METADATA"]

  environment_vars = {
    "historical_recovery_path": historical_recovery_path,
    "suffixes": tuple(set(suffixes.split(","))),
    "fetch_max_s3_keys_per_s3_listing_call": int(fetch_max_s3_keys_per_s3_listing_call),
    "last_modified_start_datetime": last_modified_start_datetime,
    "last_modified_end_datetime": last_modified_end_datetime,
    "save_result_to_s3": save_result_to_s3,
    "historical_recovery_path_metadata": historical_recovery_path_metadata
  }
  return environment_vars

def verify_not_processed_success_s3_keys(bucket, prefix, suffixes, verify_after_key, verify_to_key,
    last_modified_start_datetime, last_modified_end_datetime, fetch_max_s3_keys_per_s3_listing_call, historical_recovery_path):
  last_modified_rule = last_modified_rules[bool(last_modified_start_datetime), bool(last_modified_end_datetime)]
  kwargs = {'Bucket': bucket}
  kwargs['Prefix'] = prefix
  kwargs['MaxKeys'] = fetch_max_s3_keys_per_s3_listing_call
  if verify_after_key:
    kwargs['StartAfter'] = verify_after_key
  stop_verification = False

  logger.info('Start keys verification')

  total_verified_keys = 0
  total_created_not_processed_keys = 0

  while True:
    last_verified_key = ""
    resp = s3.list_objects_v2(**kwargs)
    contents = resp.get('Contents', [])
    if len(contents) == 0:
      logger.info("Stop verification. No keys to process returned from S3. " +
                  "Count verified keys {}. Count created not processed keys {}.".format(total_verified_keys, total_created_not_processed_keys))
      break

    for content in contents:
      key = content['Key']
      total_verified_keys = total_verified_keys + 1
      #logger.info('Verifying key {}'.format(key))
      last_verified_key = key
      last_modified_datetime = content['LastModified']
      if (key.endswith(suffixes) and
          last_modified_rule(last_modified_start_datetime, last_modified_datetime, last_modified_end_datetime) and
          verify_key_processed_success_exists_in_contents(key, contents) == False and
          verify_key_processed_success_exists_in_s3(bucket, key) == False
      ):
        create_historical_recovery_key(content, bucket, historical_recovery_path)
        total_created_not_processed_keys = total_created_not_processed_keys + 1
      if key == verify_to_key:
        stop_verification = True
        logger.info("Stop verification. Reached last key in verification range keys."
                    "Count verified keys {}. Count created not processed keys {}.".format(total_verified_keys, total_created_not_processed_keys))
        break
    if stop_verification:
      break
    kwargs['StartAfter'] = last_verified_key
  result = {
    "total_verified_keys": total_verified_keys,
    "total_created_not_processed_keys": total_created_not_processed_keys
  }
  return result

def verify_key_processed_success_exists_in_contents(verified_key, contents):
  key_chunks = verified_key.split("/")
  transaction_id = "/" + key_chunks[len(key_chunks) - 2] + "."
  #logger.info("Verifying transaction id {}".format(transaction_id))
  ods_processed_success = ".ods.processed.success"
  for content in contents:
     existing_key = content['Key']
     ods_processed_success_exists = existing_key.find(ods_processed_success)
     transaction_id_exists = existing_key.find(transaction_id)
     if transaction_id_exists > 0 and ods_processed_success_exists > 0 :
       #logger.info("Verified in s3 response that transaction id {} processed success {}".format(transaction_id, existing_key))
       return True
  return False

def verify_key_processed_success_exists_in_s3(bucket, key):
  data_response = s3.get_object(Bucket=bucket, Key=key)
  json_data_response = data_response['Body'].read().decode('utf-8')
  id = hashlib.md5(json_data_response.encode('utf-8')).hexdigest()
  processed_success_key = key.replace('/Response.json', '.' + id + '.ods.processed.success')
  try:
    #check if success marker file exists
    s3.get_object(Bucket=bucket, Key=processed_success_key)
    #exists, file processed
    #logger.info('Exists processed success key {} for file {} in bucket {} '
    #            .format(processed_success_key, key, bucket))
    return True
  except ClientError:
    #does not exist, file not processed
    #logger.info('Does not exist processed success key {} for file {} in bucket {} '
    #            .format(processed_success_key, key, bucket))
    return False

def create_historical_recovery_key(content, bucket, historical_recovery_path):
  response_file_key = content["Key"]
  last_modified_datetime = content['LastModified']
  last_modified_date = last_modified_datetime.strftime("%m-%d-%Y")
  file_name = response_file_key.rsplit('/', 1)[-2] #transactionID
  historical_recovery_key = historical_recovery_path + '/' + last_modified_date + '/' + file_name + '.txt'
  #logger.info('Generate historical recovery file {} in bucket {}'.format(historical_recovery_key, bucket))
  s3.put_object(Body=response_file_key.encode(), Bucket=bucket, Key=historical_recovery_key)

def save_metrics_to_s3(bucket, historical_recovery_path_metadata, lambda_sender_id, result, save_result_to_s3, datetime_lambda_start):
  if save_result_to_s3 == "yes" and historical_recovery_path_metadata:
    total_verified_keys = "--verified-keys-{}--".format(result["total_verified_keys"])
    total_created_not_processed_keys = "created-not-processed-keys-{}".format(result["total_created_not_processed_keys"])
    datetime_finished = "--datetime-finished-{}".format(datetime.now().strftime("%H:%M:%S"))
    lambda_time_duration = "--receiver-duration-{}".format(calculate_duration_and_format(datetime_lambda_start))
    result_key = historical_recovery_path_metadata + "/" + "receivers-results" + "/" + lambda_sender_id + total_verified_keys + \
                 total_created_not_processed_keys + datetime_finished + lambda_time_duration + "/" + lambda_sender_id + ".txt"
    s3.put_object(Body="", Bucket=bucket, Key=result_key)

def calculate_duration(datetime_start):
  datetime_end = datetime.now()
  return int((datetime_end - datetime_start).total_seconds())

def calculate_duration_and_format(datetime_start):
  return format_seconds(calculate_duration(datetime_start))

def format_seconds(seconds):
  if seconds <= 0:
    return "00 hours 00 min 00 sec"
  else:
    return time.strftime("%H hours %M min %S sec", time.gmtime(seconds))

def lambda_handler(event, context):
  datetime_lambda_start = datetime.now()

  json_event = json.dumps(event)
  logger.info("Lambda received event {} ".format(json_event))

  environment_vars = read_and_validate_environment_vars()
  historical_recovery_path = environment_vars["historical_recovery_path"]
  suffixes = environment_vars["suffixes"]
  fetch_max_s3_keys_per_s3_listing_call = environment_vars["fetch_max_s3_keys_per_s3_listing_call"]
  last_modified_start_datetime = environment_vars["last_modified_start_datetime"]
  last_modified_end_datetime = environment_vars["last_modified_end_datetime"]
  save_result_to_s3 = environment_vars["save_result_to_s3"]
  historical_recovery_path_metadata = environment_vars["historical_recovery_path_metadata"]

  for record in event['Records']:
    body = json.loads(record["body"])
    bucket = body["bucket"]
    prefix = body["prefix"]
    lambda_sender_id = body["id"]
    verify_after_key = body["verifyAfterKey"]
    verify_to_key = body["verifyToKey"]

    result = verify_not_processed_success_s3_keys(bucket, prefix, suffixes, verify_after_key, verify_to_key, last_modified_start_datetime,
                                                  last_modified_end_datetime, fetch_max_s3_keys_per_s3_listing_call, historical_recovery_path)
    save_metrics_to_s3(bucket, historical_recovery_path_metadata, lambda_sender_id, result, save_result_to_s3, datetime_lambda_start)

  return "success"