import os
import logging
import boto3
import json
import time

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

  fetch_max_s3_keys_per_s3_listing_call = os.environ["FETCH_MAX_S3_KEYS_PER_S3_LISTING_CALL"]
  if not fetch_max_s3_keys_per_s3_listing_call:
    raise Exception("fetch_max_s3_keys_per_s3_listing_call is not defined")

  lambda_working_limit_seconds = os.environ["LAMBDA_WORKING_LIMIT_SECONDS"]
  if not lambda_working_limit_seconds:
    raise Exception("lambda_working_limit_seconds is not defined")

  environment_vars = {
    "bucket": bucket_name,
    "historical_recovery_path": historical_recovery_path,
    "fetch_max_s3_keys_per_s3_listing_call": int(fetch_max_s3_keys_per_s3_listing_call),
    "lambda_working_limit_seconds": int(lambda_working_limit_seconds)
  }
  return environment_vars

def do_async_lambda_call(action, lambda_async_number, prefix, last_verified_key, function_name,
    total_time_flow_execution_seconds, total_keys_success_per_prefix, total_keys_failed_per_prefix):
  payload = json.dumps({"action": action,
                        "lambdaAsyncNumber": lambda_async_number,
                        "prefix": prefix,
                        "lastVerifiedKey": last_verified_key,
                        "totalTimeFlowExecutionSeconds": total_time_flow_execution_seconds,
                        "totalKeysSuccessPerPrefix": total_keys_success_per_prefix,
                        "totalKeysFailedPerPrefix": total_keys_failed_per_prefix
                        })
  lambda_client.invoke(
      FunctionName=function_name,
      InvocationType='Event',
      Payload=payload
  )

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

def verify_lambda_exec_time_exceeded(datetime_lambda_start, lambda_working_limit_seconds):
  lambda_exec_seconds = calculate_duration(datetime_lambda_start)
  if(lambda_exec_seconds < lambda_working_limit_seconds):
    return False
  return True

def create_prefixes(start_prefix, end_prefix):
  start_prefix = int(start_prefix)
  end_prefix = int(end_prefix)
  prefixes = []
  for prefix in range(start_prefix, end_prefix + 1):
      prefixes.append(prefix)
  logger.info('Created {} prefixes {}'.format(len(prefixes), prefixes))
  return tuple(prefixes)

def log_metrics(bucket, prefix, lambda_async_number, lambda_time_execution, total_time_flow_execution, process_after_key_name, last_verified_key,
    keys_processed_success_per_lambda, keys_processed_failed_per_lambda, total_keys_success_per_prefix, total_keys_failed_per_prefix):
  metrics = json.dumps({"bucket": bucket,
                        "prefix": prefix,
                        "lambdaAsyncNumber": lambda_async_number,
                        "timeLambdaExecution": format_seconds(lambda_time_execution),
                        "totalTimeFlowExecution": format_seconds(total_time_flow_execution),
                        "verifiedKeyFromName": process_after_key_name,
                        "verifiedKeyToName": last_verified_key,
                        "keysProcessedSuccessPerLambda": keys_processed_success_per_lambda,
                        "keysProcessedFailedPerLambda": keys_processed_failed_per_lambda,
                        "totalKeysSuccessPerPrefix": total_keys_success_per_prefix,
                        "totalKeysFailedPerPrefix": total_keys_failed_per_prefix
                        }
                      )
  logger.info('Finished recovery keys processing {}'.format(metrics))

def lambda_handler(event, context):
  datetime_lambda_start = datetime.now()

  action_process_prefixes = "process_prefixes"
  action_process_prefix = "process_prefix"

  json_str = json.dumps(event)
  logger.info("Lambda received event {} ".format(json_str))
  json_object = json.loads(json_str)

  environment_vars = read_and_validate_environment_vars()
  bucket = environment_vars["bucket"]
  historical_recovery_path = environment_vars["historical_recovery_path"]
  fetch_max_s3_keys_per_s3_listing_call = environment_vars["fetch_max_s3_keys_per_s3_listing_call"]
  lambda_working_limit_seconds = environment_vars["lambda_working_limit_seconds"]
  action = json_object['action']

  if action == action_process_prefixes:
    prefix_date = ""
    start_prefix = ""
    end_prefix = ""
    prefixes = []

    try:
      prefix_date = json_object['prefixDate']
    except KeyError:
      msg = "prefixDate must be sent in request payload"
      logger.error(msg)
      return msg

    try:
      start_prefix = json_object['startPrefixNumber']
      end_prefix = json_object['endPrefixNumber']
    except KeyError:
      logger.info("start_prefix and end_prefix not defined in request")

    if start_prefix and end_prefix:
      prefixes = create_prefixes(start_prefix, end_prefix)
    else:
      prefixes.append(prefix_date)

    logger.info('Started to process historical data with configuration: '
                'bucket {} ; historical_recovery_path {} ; '
                'fetch_max_s3_keys_per_s3_listing_call {} ; prefixes count {} ; '
                'prefix_date {} ; lambda_working_limit_seconds {}'
                .format(bucket, historical_recovery_path, fetch_max_s3_keys_per_s3_listing_call,
                        len(prefixes), prefix_date, lambda_working_limit_seconds))

    for prefix in prefixes:
      if start_prefix and end_prefix:
        prefix_path = historical_recovery_path + "/" + prefix_date + "/" + str(prefix)
      else:
        prefix_path = historical_recovery_path + "/" + prefix_date
      do_async_lambda_call(action_process_prefix, 1, prefix_path, "", context.function_name, 0, 0, 0)
  elif action == action_process_prefix:
    start_after = json_object["lastVerifiedKey"]
    lambda_async_number = json_object["lambdaAsyncNumber"]
    prefix = json_object["prefix"]
    total_time_flow_execution_seconds = json_object["totalTimeFlowExecutionSeconds"]
    total_keys_success_per_prefix = json_object["totalKeysSuccessPerPrefix"]
    total_keys_failed_per_prefix = json_object["totalKeysFailedPerPrefix"]

    kwargs = {'Bucket': bucket}
    kwargs['Prefix'] = prefix
    kwargs['MaxKeys'] = fetch_max_s3_keys_per_s3_listing_call
    if start_after:
      kwargs['StartAfter'] = start_after
    last_verified_key = ""
    stop_processing = False
    keys_success_per_lambda = 0
    keys_failed_per_lambda = 0

    while stop_processing == False:
      resp = s3.list_objects_v2(**kwargs)
      contents = resp.get('Contents', [])
      len_contents = len(contents)
      if len_contents == 0:
        lambda_time_duration = calculate_duration(datetime_lambda_start)
        total_time_flow_execution_seconds = total_time_flow_execution_seconds + lambda_time_duration
        total_keys_success_per_prefix = total_keys_success_per_prefix + keys_success_per_lambda
        total_keys_failed_per_prefix = total_keys_failed_per_prefix + keys_failed_per_lambda
        log_metrics(bucket, prefix, lambda_async_number, lambda_time_duration, total_time_flow_execution_seconds, start_after, last_verified_key,
                  keys_success_per_lambda, keys_failed_per_lambda, total_keys_success_per_prefix, total_keys_failed_per_prefix)
        break

      logger.info('contents {}'.format(contents))
      #sleep_test(2)
      for content in contents:
        key = content['Key']
        data_response = s3.get_object(Bucket=bucket, Key=key)
        json_data_response = data_response['Body'].read().decode('utf-8')
        is_recovery_key_removed = process_file(bucket, key)
        if is_recovery_key_removed == False:
          last_verified_key = key
          keys_failed_per_lambda = keys_failed_per_lambda + 1
        else:
          keys_success_per_lambda = keys_success_per_lambda + 1
        stop_processing = verify_lambda_exec_time_exceeded(datetime_lambda_start, lambda_working_limit_seconds)
        if stop_processing == True:
          lambda_time_duration = calculate_duration(datetime_lambda_start)
          total_time_flow_execution_seconds = total_time_flow_execution_seconds + lambda_time_duration
          total_keys_success_per_prefix = total_keys_success_per_prefix + keys_success_per_lambda
          total_keys_failed_per_prefix = total_keys_failed_per_prefix + keys_failed_per_lambda

          do_async_lambda_call(action_process_prefix, lambda_async_number + 1, prefix, last_verified_key, context.function_name,
                               total_time_flow_execution_seconds, total_keys_success_per_prefix, total_keys_failed_per_prefix)

          log_metrics(bucket, prefix, lambda_async_number, lambda_time_duration, total_time_flow_execution_seconds, start_after, last_verified_key,
                      keys_success_per_lambda, keys_failed_per_lambda, total_keys_success_per_prefix, total_keys_failed_per_prefix)

          break
      kwargs['StartAfter'] = last_verified_key
  else:
    logger.info('Action not supported {}'.format(action))

  return "success"

def sleep_test(seconds):
  logger.info("before sleep")
  time.sleep(seconds)
  logger.info("after sleep")

def process_file(bucket, key):
  # found1 = key.find("11111111.txt")
  # found2 = key.find("333333333.txt")
  # if found1 > 0 or found2 > 0:
    s3.delete_object(Bucket=bucket, Key=key)
    logger.info('Deleted key {}'.format(key))
    return True
  # else:
  #   logger.info('Not deleted key {}'.format(key))
  #return False;
