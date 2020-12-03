import os
import logging
import boto3
import json
import time

from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')
sqs_client = boto3.client('sqs')

def process_prefix_async_call(action, lambda_async_number, prefix, last_verified_key,
    datetime_lambda_start, total_time_flow_seconds, total_keys_processed, function_name):
  total_time_flow_seconds = total_time_flow_seconds + calculate_duration(datetime_lambda_start)
  payload = json.dumps({"action": action,
                        "lambdaAsyncNumber": lambda_async_number,
                        "totalTimeFlowExecution": format_seconds(total_time_flow_seconds),
                        "totalTimeFlowSeconds": total_time_flow_seconds,
                        "totalKeysProcessed": total_keys_processed,
                        "prefix": prefix,
                        "lastVerifiedKey": last_verified_key
                        })
  logger.info("New lambda asynchronous call")
  lambda_client.invoke(
      FunctionName=function_name,
      InvocationType='Event',
      Payload=payload
  )

def read_and_validate_environment_vars():
  bucket = os.environ["BUCKET"]
  if not bucket:
    raise Exception("bucket is not defined")

  prefixes = os.environ["PREFIXES"]
  if not prefixes:
    raise Exception("prefixes are not defined")

  sqs_keys_queue_url = os.environ["SQS_KEYS_QUEUE_URL"]
  if not sqs_keys_queue_url:
    raise Exception("sqs_keys_queue_url is not defined")

  fetch_max_s3_keys_per_s3_listing_call = os.environ["FETCH_MAX_S3_KEYS_PER_S3_LISTING_CALL"]
  if not fetch_max_s3_keys_per_s3_listing_call:
    raise Exception("fetch_max_s3_keys_per_s3_listing_call is not defined")

  lambda_working_limit_seconds = os.environ["LAMBDA_WORKING_LIMIT_SECONDS"]
  if not lambda_working_limit_seconds:
    raise Exception("lambda_working_limit_seconds is not defined")

  environment_vars = {
    "bucket": bucket,
    "prefixes": tuple(set(prefixes.split(","))),
    "sqs_keys_queue_url": sqs_keys_queue_url,
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

def send_message_to_sqs(sqs_keys_queue_url, sqs_msg_payload):
  logger.info("Request to SQS {}".format(sqs_msg_payload))
  response = sqs_client.send_message(QueueUrl=sqs_keys_queue_url, MessageBody=sqs_msg_payload)
  logger.info("Response from SQS {}".format(response))

def create_sqs_msg_payload(bucket, prefix, lambda_async_number, iteration, verify_after_key, verify_to_key):
  return json.dumps({
                     "bucket": bucket,
                     "prefix": prefix,
                     "id": "id-{}-{}".format(lambda_async_number, iteration),
                     "verifyAfterKey": verify_after_key,
                     "verifyToKey": verify_to_key
                     }
                   )

def calculate_duration(datetime_list_objects_start):
  datetime_list_objects_end = datetime.now()
  return int((datetime_list_objects_end - datetime_list_objects_start).total_seconds())

def calculate_duration_and_format(datetime_list_objects_start):
  return format_seconds(calculate_duration(datetime_list_objects_start))

def format_seconds(seconds):
  if seconds <= 0:
    return "00 hours 00 min 00 sec"
  else:
    return time.strftime("%H hours %M min %S sec", time.gmtime(seconds))

def lambda_handler(event, context):
  datetime_lambda_start = datetime.now()

  action_process_prefix = "process_prefix"
  action_process_prefixes = "process_prefixes"

  json_str = json.dumps(event)
  logger.info("Lambda received event {} ".format(json_str))
  json_object = json.loads(json_str)

  environment_vars = read_and_validate_environment_vars()
  bucket = environment_vars["bucket"]
  prefixes = environment_vars["prefixes"]
  fetch_max_s3_keys_per_s3_listing_call = environment_vars["fetch_max_s3_keys_per_s3_listing_call"]
  lambda_working_limit_seconds = environment_vars["lambda_working_limit_seconds"]
  sqs_keys_queue_url = environment_vars["sqs_keys_queue_url"]
  action = json_object['action']

  if action == action_process_prefixes:
    logger.info('Started to process historical data with configuration: '
                'bucket {} ; prefixes {} ; '
                'fetch_max_s3_keys_per_s3_listing_call {} ; lambda_working_limit_seconds {}'
                .format(bucket, prefixes, fetch_max_s3_keys_per_s3_listing_call, lambda_working_limit_seconds))

    for prefix in prefixes:
      process_prefix_async_call(action_process_prefix, 1, prefix, "", datetime_lambda_start, 0, 0, context.function_name)

  elif action == action_process_prefix:
    lambda_async_number = json_object['lambdaAsyncNumber']
    total_time_flow_seconds = json_object['totalTimeFlowSeconds']
    total_keys_processed = json_object['totalKeysProcessed']
    prefix = json_object['prefix']
    start_after_key = json_object['lastVerifiedKey']

    kwargs = {'Bucket': bucket}
    kwargs['Prefix'] = prefix
    kwargs['MaxKeys'] = fetch_max_s3_keys_per_s3_listing_call
    if start_after_key:
      kwargs['StartAfter'] = start_after_key

    last_verified_key = ""
    iteration = 1
    while lambda_exec_time_limit_not_exceeded(datetime_lambda_start, lambda_working_limit_seconds) == True:
      datetime_list_objects_start = datetime.now()
      s3_response = s3_client.list_objects_v2(**kwargs)
      list_objects_v2_duration = calculate_duration_and_format(datetime_list_objects_start)

      s3_contents = s3_response.get('Contents', [])
      len_s3_contents = len(s3_contents)
      logger.info("Started iteration {}. S3 returned {} objects for {}.".format(iteration, len_s3_contents, list_objects_v2_duration))
      if len_s3_contents == 0:
        logger.info("No keys to process returned from S3. Iteration {}. Lambda async number {}.".format(iteration, lambda_async_number))
        return "success"
      else:
        if lambda_async_number == 1 and iteration == 1:
          verify_after_key = ""
        else:
          verify_after_key = start_after_key if iteration == 1 else last_verified_key
        verify_to_key = s3_contents[len_s3_contents - 1]['Key']
        kwargs['StartAfter'] = verify_to_key
        last_verified_key = verify_to_key

        sqs_msg_payload = create_sqs_msg_payload(bucket, prefix, lambda_async_number, iteration, verify_after_key, verify_to_key)
        send_message_to_sqs(sqs_keys_queue_url, sqs_msg_payload)
        sleep_test(5)
        total_keys_processed = total_keys_processed + len_s3_contents
      iteration = iteration + 1
    process_prefix_async_call(action_process_prefix, lambda_async_number + 1, prefix, last_verified_key,
                              datetime_lambda_start, total_time_flow_seconds, total_keys_processed, context.function_name)

  return "success"

def sleep_test(seconds):
  logger.info("before sleep")
  time.sleep(seconds)
  logger.info("after sleep")