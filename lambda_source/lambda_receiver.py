import os
import logging
import boto3
import hashlib
import json
import time

from botocore.exceptions import ClientError
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

def read_and_validate_environment_vars():
  historical_recovery_path = os.environ["HISTORICAL_RECOVERY_PATH"]
  if not historical_recovery_path:
    raise Exception("historical_recovery_path is not defined")

  fetch_max_s3_keys_per_s3_listing_call = os.environ["FETCH_MAX_S3_KEYS_PER_S3_LISTING_CALL"]
  if not fetch_max_s3_keys_per_s3_listing_call:
    raise Exception("fetch_max_s3_keys_per_s3_listing_call is not defined")

  lambda_working_limit_seconds = os.environ["LAMBDA_WORKING_LIMIT_SECONDS"]
  if not lambda_working_limit_seconds:
    raise Exception("lambda_working_limit_seconds is not defined")

  environment_vars = {
    "historical_recovery_path": historical_recovery_path,
    "fetch_max_s3_keys_per_s3_listing_call": int(fetch_max_s3_keys_per_s3_listing_call),
    "lambda_working_limit_seconds": int(lambda_working_limit_seconds)
  }
  return environment_vars

def lambda_exec_time_limit_not_exceeded(datetime_lambda_start, lambda_working_limit_seconds):
  current_datetime_lambda = datetime.now()
  lambda_exec_seconds = int((current_datetime_lambda - datetime_lambda_start).total_seconds())
  if(lambda_exec_seconds < lambda_working_limit_seconds):
    return True
  return False

def verify_not_processed_success_s3_keys(bucket, prefix, processing_date, verify_after_key, verify_to_key,
    fetch_max_s3_keys_per_s3_listing_call, historical_recovery_path, datetime_lambda_start,
    lambda_working_limit_seconds, total_verified_keys, total_created_not_processed_keys):

  kwargs = {'Bucket': bucket}
  kwargs['Prefix'] = prefix + "/" + processing_date
  kwargs['MaxKeys'] = fetch_max_s3_keys_per_s3_listing_call
  if verify_after_key:
    kwargs['StartAfter'] = verify_after_key
  stop_verification = False

  logger.info('Start keys verification')

  proceed_processing = False

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
      if not "ods.processed.success" in key:
        if (verify_key_processed_success_exists_in_contents(key, contents) == False and
            verify_key_processed_success_exists_in_s3(bucket, key) == False):
              create_historical_recovery_key(content, bucket, historical_recovery_path)
              total_created_not_processed_keys = total_created_not_processed_keys + 1
      if key == verify_to_key:
        stop_verification = True
        logger.info("Stop verification. Reached last key in verification range keys."
                    "Count verified keys {}. Count created not processed keys {}.".format(total_verified_keys, total_created_not_processed_keys))
        break
      sleep_test(2)
      if lambda_exec_time_limit_not_exceeded(datetime_lambda_start, lambda_working_limit_seconds) == False:
        stop_verification = True
        proceed_processing = True
        logger.info("Stop verification. Lambda execution timeout limit exceeded."
                    "Count verified keys {}. Count created not processed keys {}.".format(total_verified_keys, total_created_not_processed_keys))
        break
    if stop_verification:
      break
    kwargs['StartAfter'] = last_verified_key
  result = {
    "total_verified_keys": total_verified_keys,
    "total_created_not_processed_keys": total_created_not_processed_keys,
    "proceed_processing": proceed_processing,
    "last_verified_key": last_verified_key
  }
  return result

def get_transaction_id(key):
  key_chunks = key.split("/")
  transaction_id = key_chunks[len(key_chunks) - 1]
  if transaction_id.endswith(".json"):
    return transaction_id.replace(".json", "")
  return transaction_id

def create_historical_recovery_key_path(historical_recovery_path, key, last_modified_datetime):
  file_name = None
  if key.endswith(".json"):
    file_name = key.replace('.json', '')
  else:
    file_name = key
  key_chunks = key.split("/")
  file_name = file_name.replace(key_chunks[1], key_chunks[1] + "/" + str(last_modified_datetime.hour))
  historical_recovery_key_path = historical_recovery_path + "/" + file_name + '.txt'
  return historical_recovery_key_path

def verify_key_processed_success_exists_in_contents(verified_key, contents):
  transaction_id = get_transaction_id(verified_key)
  logger.info("Verifying S3 response's content if 'processed success' exists for key {}".format(verified_key))
  ods_processed_success = ".ods.processed.success"
  for content in contents:
     existing_key = content['Key']
     ods_processed_success_exists = existing_key.find(ods_processed_success)
     transaction_id_exists = existing_key.find(transaction_id)
     if transaction_id_exists > 0 and ods_processed_success_exists > 0 :
       logger.info("Exists in S3 response's content 'processed success' key {}".format(existing_key))
       return True
  logger.info("Does not exists in S3 response's content 'processed success' key")
  return False

def verify_key_processed_success_exists_in_s3(bucket, key):
  logger.info("Verifying calling S3 if 'processed success' exists for key {}".format(key))
  data_response = s3.get_object(Bucket=bucket, Key=key)
  json_data_response = data_response['Body'].read().decode('utf-8')
  id = hashlib.md5(json_data_response.encode('utf-8')).hexdigest()
  processed_success_key = None
  if key.endswith(".json"):
    processed_success_key = key.replace('.json', '.' + id + '.ods.processed.success')
  else:
    processed_success_key = key + '.' + id + '.ods.processed.success'
  try:
    #check if success marker file exists
    s3.get_object(Bucket=bucket, Key=processed_success_key)
    #exists, file processed
    logger.info("Exists 'processed success' key {}".format(processed_success_key))
    return True
  except ClientError:
    #does not exist, file not processed
    logger.info("Does not exist 'processed success' key {}".format(processed_success_key))
    return False

def create_historical_recovery_key(content, bucket, historical_recovery_path):
  response_file_key = content["Key"]
  last_modified_datetime = content['LastModified']
  historical_recovery_key = create_historical_recovery_key_path(historical_recovery_path, response_file_key, last_modified_datetime)
  logger.info('Generate historical recovery file {} in bucket {}'.format(historical_recovery_key, bucket))
  s3.put_object(Body=response_file_key.encode(), Bucket=bucket, Key=historical_recovery_key)

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

def log_metrics(bucket, prefix, result, lambda_sender_id, datetime_lambda_start, processing_date):
  metrics = json.dumps({
    "bucket": bucket,
    "prefix": prefix,
    "processingDate": processing_date,
    "id": str(lambda_sender_id) + "-" + prefix,
    "lambdaStartTime": datetime_lambda_start.strftime("%m-%d-%Y %H:%M:%S"),
    "lambdaEndTime": datetime.now().strftime("%m-%d-%Y %H:%M:%S"),
    "lambdaDuration": calculate_duration_and_format(datetime_lambda_start),
    "totalVerifiedKeys": result["total_verified_keys"],
    "totalCreatedNotProcessedSuccessKeys": result["total_created_not_processed_keys"]
    }
  )
  logger.info('Receiver finished message processing {}'.format(metrics))

def process_prefix_async_call(lambda_sender_id, bucket, prefix, processing_date, verify_after_key, verify_to_key,
    total_verified_keys, total_created_not_processed_keys, function_name):
  payload = json.dumps({"isAsyncCall": True,
                        "lambdaSenderId": lambda_sender_id + "-" + prefix,
                        "bucket": bucket,
                        "prefix": prefix,
                        "processingDate": processing_date,
                        "verifyAfterKey": verify_after_key,
                        "verifyToKey": verify_to_key,
                        "totalVerifiedKeys": total_verified_keys,
                        "totalCreatedNotProcessedKeys": total_created_not_processed_keys
                        })
  lambda_client.invoke(
      FunctionName=function_name,
      InvocationType='Event',
      Payload=payload
  )

def lambda_handler(event, context):
  datetime_lambda_start = datetime.now()

  json_event = json.dumps(event)
  logger.info("Lambda received event {} ".format(json_event))

  environment_vars = read_and_validate_environment_vars()
  historical_recovery_path = environment_vars["historical_recovery_path"]
  fetch_max_s3_keys_per_s3_listing_call = environment_vars["fetch_max_s3_keys_per_s3_listing_call"]
  lambda_working_limit_seconds = environment_vars["lambda_working_limit_seconds"]

  bucket = None
  prefix = None
  processing_date = None
  lambda_sender_id = None
  verify_after_key = None
  verify_to_key = None
  total_verified_keys = None
  total_created_not_processed_keys = None

  is_async_call = False
  try:
    is_async_call = event['isAsyncCall']
  except:
    logger.info("is_async_call {}".format(is_async_call))
    is_async_call = False

  if is_async_call:
    logger.info("received event from async call")
    bucket = event["bucket"]
    prefix = event["prefix"]
    processing_date = event["processingDate"]
    lambda_sender_id = event["lambdaSenderId"]
    verify_after_key = event["verifyAfterKey"]
    verify_to_key = event["verifyToKey"]
    total_verified_keys = event["totalVerifiedKeys"]
    total_created_not_processed_keys = event["totalCreatedNotProcessedKeys"]
  else:
    logger.info("received event from SQS")
    for record in event['Records']:
      body = json.loads(record["body"])
      bucket = body["bucket"]
      prefix = body["prefix"]
      processing_date = body["processingDate"]
      lambda_sender_id = body["id"]
      verify_after_key = body["verifyAfterKey"]
      verify_to_key = body["verifyToKey"]
      total_verified_keys = 0
      total_created_not_processed_keys = 0
      break

  result = verify_not_processed_success_s3_keys(bucket, prefix, processing_date, verify_after_key, verify_to_key, fetch_max_s3_keys_per_s3_listing_call,
                                                historical_recovery_path, datetime_lambda_start, lambda_working_limit_seconds,
                                                total_verified_keys, total_created_not_processed_keys)
  log_metrics(bucket, prefix, result, lambda_sender_id, datetime_lambda_start, processing_date)

  proceed_processing = result["proceed_processing"]
  last_verified_key =  result["last_verified_key"]
  if proceed_processing:
    process_prefix_async_call(lambda_sender_id, bucket, prefix, processing_date, last_verified_key, verify_to_key,
                              result["total_verified_keys"], result["total_created_not_processed_keys"], context.function_name)

  return "success"

def sleep_test(seconds):
  logger.info("before sleep")
  time.sleep(seconds)
  logger.info("after sleep")