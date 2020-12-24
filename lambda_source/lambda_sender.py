import os
import logging
import boto3
import json
import time

from botocore.exceptions import ClientError
from datetime import timedelta, datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')
sqs_client = boto3.client('sqs')


def async_call(action, lambda_async_number, prefix, start_date, end_date, current_processing_date, last_verified_key,
    datetime_lambda_start, total_time_flow_seconds, total_keys_processed, function_name):

  total_time_flow_seconds = total_time_flow_seconds + calculate_duration(datetime_lambda_start)
  payload = json.dumps({"action": action,
                        "lambdaAsyncNumber": lambda_async_number,
                        "totalTimeFlowExecution": format_seconds(total_time_flow_seconds),
                        "totalTimeFlowSeconds": total_time_flow_seconds,
                        "totalKeysProcessed": total_keys_processed,
                        "prefix": prefix,
                        "startDate": start_date,
                        "endDate": end_date,
                        "currentProcessingDate": current_processing_date,
                        "lastVerifiedKey": last_verified_key
                        })
  logger.info("New lambda asynchronous call")
  lambda_client.invoke(
      FunctionName=function_name,
      InvocationType='Event',
      Payload=payload
  )

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

  historical_recovery_path_metadata = os.environ["HISTORICAL_RECOVERY_PATH_METADATA"]

  environment_vars = {
    "bucket": bucket,
    "prefixes": tuple(set(prefixes.split(","))),
    "sqs_keys_queue_url": sqs_keys_queue_url,
    "fetch_max_s3_keys_per_s3_listing_call": int(fetch_max_s3_keys_per_s3_listing_call),
    "lambda_working_limit_seconds": int(lambda_working_limit_seconds),
    "historical_recovery_path_metadata": historical_recovery_path_metadata
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

def verify_stop_flow_execution_flag(bucket, historical_recovery_path_metadata, prefix):
  try:
    key = historical_recovery_path_metadata + "/" + prefix + "/" + "stop.flag"
    s3_client.get_object(Bucket=bucket, Key=key)
    logger.info('Stop flow execution for bucket {} and prefix {}'.format(bucket, prefix))
    return True
  except ClientError:
    return False

def validate_request(request):
  prefix = None
  start_date = None
  end_date = None
  start_datetime = None
  end_datetime = None

  try:
    prefix = request['prefix']
    if not prefix:
      raise KeyError
  except KeyError:
    message = "Error: 'prefix' is not sent in request payload"
    logger.error(message)
    return {
      "status": "error",
      "message": message
    }

  try:
    start_date = request['startDate']
    if not start_date:
      raise KeyError
  except KeyError:
    message = "Error: 'startDate' is not sent in request payload"
    logger.error(message)
    return {
      "status": "error",
      "message": message
    }

  try:
    end_date = request['endDate']
  except KeyError:
    end_date = start_date
    logger.info("'endDate' is not sent in payload. only 'startDate' will be scanned.")

  try:
    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
  except ValueError:
    message = "Error: Incorrect format for 'startDate'. Expected 'yyyy-mm-dd' format"
    logger.error(message)
    return {
      "status": "error",
      "message": message
    }

  try:
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
  except ValueError:
    message = "Error: Incorrect format for 'endDate'. Expected 'yyyy-mm-dd' format"
    logger.error(message)
    return {
      "status": "error",
      "message": message
    }

  if end_datetime < start_datetime:
    message = "Error: 'endDate' is less than 'startDate'"
    logger.error(message)
    return {
      "status": "error",
      "message": message
    }

  return {
      "status": "success",
      "prefix": prefix,
      "start_date": start_date,
      "end_date": end_date
  }

def get_next_processing_date(current_processing_date):
  current_processing_datetime = datetime.strptime(current_processing_date, '%Y-%m-%d')
  next_processing_datetime = current_processing_datetime + timedelta(1)
  next_processing_date = next_processing_datetime.strftime('%Y-%m-%d')
  return next_processing_date

def lambda_handler(event, context):
  datetime_lambda_start = datetime.now()

  action_process_prefix = "process_prefix"
  action_process_prefixes = "process_prefixes"

  json_str = json.dumps(event)
  logger.info("Lambda received event {} ".format(json_str))
  request = json.loads(json_str)

  environment_vars = read_and_validate_environment_vars()
  bucket = environment_vars["bucket"]
  prefixes = environment_vars["prefixes"]
  fetch_max_s3_keys_per_s3_listing_call = environment_vars["fetch_max_s3_keys_per_s3_listing_call"]
  lambda_working_limit_seconds = environment_vars["lambda_working_limit_seconds"]
  sqs_keys_queue_url = environment_vars["sqs_keys_queue_url"]
  historical_recovery_path_metadata = environment_vars["historical_recovery_path_metadata"]

  action = None
  prefix = None
  start_date = None
  end_date = None
  start_datetime = None
  end_datetime = None
  action = None

  try:
    action = request['action']
    if action != "start_processing" and action != "proceed_processing":
      raise KeyError
  except KeyError:
    message = "Error: 'action' has to be sent in request payload with 'start_processing' or 'proceed_processing' value"
    logger.error(message)
    return message

  if action == "start_processing":
    result = validate_request(request)
    status = result["status"]
    if status == "error":
      return result["message"]
    else:
      logger.info("start processing data for 'prefix' {} from 'startDate' {} to 'endDate' {}"
                .format(result["prefix"], result["start_date"], result["end_date"]))
      async_call("proceed_processing", 1, result["prefix"], result["start_date"], result["end_date"], result["start_date"],
                 "", datetime_lambda_start, 0, 0, context.function_name)
  elif action == "proceed_processing":
    logger.info("proceed_processing")
    lambda_async_number = request['lambdaAsyncNumber']
    total_time_flow_seconds = request['totalTimeFlowSeconds']
    total_keys_processed = request['totalKeysProcessed']
    prefix = request['prefix']
    current_processing_date = request['currentProcessingDate']
    start_date = request['endDate']
    end_date = request['endDate']
    prefix = request['prefix']
    start_after_key = request['lastVerifiedKey']
    processing_date = current_processing_date

    stop_flow_execution = verify_stop_flow_execution_flag(bucket, historical_recovery_path_metadata, prefix)
    if stop_flow_execution:
      return "success"

    kwargs = {'Bucket': bucket}
    kwargs['Prefix'] = prefix + "/" + current_processing_date
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
        if current_processing_date == end_date:
          logger.info("No keys to process returned from S3. Finished to process last date {}.".format(current_processing_date))
          return "success"
        else:
          processing_date = get_next_processing_date(current_processing_date)
          last_verified_key = ""
          logger.info("next processing date {}.".format(processing_date))
          break
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
        #sleep_test(10)
        total_keys_processed = total_keys_processed + len_s3_contents
      iteration = iteration + 1
    async_call("proceed_processing", lambda_async_number + 1, prefix, start_date, end_date, processing_date, last_verified_key,
                            datetime_lambda_start, total_time_flow_seconds, total_keys_processed, context.function_name)

  else:
    message = "Error: '" + action + "' not supported"
    logger.error(message)
    return message

  return "success"