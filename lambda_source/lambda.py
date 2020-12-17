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

def verify_not_processed_success_s3_keys(bucket, prefix, suffixes, process_after_key_name, s3_keys_listing_limit_per_call,
    last_modified_start_datetime, last_modified_end_datetime, datetime_lambda_start,
    lambda_execution_limit_seconds, historical_recovery_path,
    total_not_processed_success_keys_per_prefix, max_keys_per_historical_path_prefix):
  # Use the last_modified_rules dict to lookup which conditional logic to apply
  # based on which arguments were supplied
  last_modified_rule = last_modified_rules[bool(last_modified_start_datetime), bool(last_modified_end_datetime)]

  kwargs = {'Bucket': bucket}
  kwargs['Prefix'] = prefix
  kwargs['MaxKeys'] = s3_keys_listing_limit_per_call
  if process_after_key_name:
    kwargs['StartAfter'] = process_after_key_name
    logger.info("Start to process prefix {} in bucket {} after key name {}".format(prefix, bucket, process_after_key_name))
  else:
    logger.info("Start to process prefix {} in bucket {} from first key".format(prefix, bucket))

  last_verified_key = ""
  not_processed_success_keys = 0
  time_exec_list_objects_v2 = []
  total_time_exec_list_objects_v2 = 0
  total_verified_keys_per_lambda = 0
  total_time_verify_all_keys = 0
  stop_processing = False
  iteration = 0
  while stop_processing == False:
    iteration = iteration + 1

    datetime_list_objects_start = datetime.now()
    #sleep_test((iteration ) * 5)
    resp = s3.list_objects_v2(**kwargs)
    datetime_list_objects_end = datetime.now()
    datetime_list_objects_time_execution = int((datetime_list_objects_end - datetime_list_objects_start).total_seconds())
    total_time_exec_list_objects_v2 = total_time_exec_list_objects_v2 + datetime_list_objects_time_execution

    contents = resp.get('Contents', [])
    len_contents = len(contents)
    time_exec_list_objects_v2.append("{} call returned {} keys for {}"
                                     .format(iteration, len_contents, format_seconds(datetime_list_objects_time_execution)))
    if len_contents == 0:
      logger.info("No keys to process returned from S3. Break iteration.")
      break
    datetime_verify_list_keys_start = datetime.now()
    for content in contents:
      #sleep_test(4)
      total_verified_keys_per_lambda = total_verified_keys_per_lambda + 1
      file_key = content['Key']
      #logger.info('Verifying key {}'.format(file_key))
      last_verified_key = file_key
      last_modified_datetime = content['LastModified']
      if (file_key.endswith(suffixes) and
          last_modified_rule(last_modified_start_datetime, last_modified_datetime, last_modified_end_datetime) and
          verify_key_processed_success_exists_in_contents(file_key, contents) == False and
          verify_key_processed_success_exists_in_s3(bucket, file_key) == False
      ):
        not_processed_success_keys = not_processed_success_keys + 1
        total_keys = total_not_processed_success_keys_per_prefix + not_processed_success_keys
        historical_path_prefix = create_historical_path_prefix(total_keys, max_keys_per_historical_path_prefix)
        create_historical_recovery_key(content, bucket, historical_recovery_path, historical_path_prefix)

      current_datetime_lambda = datetime.now()
      lambda_exec_seconds = int((current_datetime_lambda - datetime_lambda_start).total_seconds())
      if(lambda_exec_seconds > lambda_execution_limit_seconds):
        stop_processing = True
        break
    kwargs['StartAfter'] = last_verified_key
    datetime_verify_list_keys_end = datetime.now()
    datetime_verify_list_keys_time_exec = int((datetime_verify_list_keys_end - datetime_verify_list_keys_start).total_seconds())
    total_time_verify_all_keys = total_time_verify_all_keys + datetime_verify_list_keys_time_exec

  result = {
    "TotalVerifiedKeysPerLambda": total_verified_keys_per_lambda,
    "LastVerifiedKey": last_verified_key,
    "NotProcessedSuccessKeys": not_processed_success_keys,
    "TotalTimeExecListObjectsV2": format_seconds(total_time_exec_list_objects_v2),
    "TimeExecListObjectsV2PerCall": tuple(time_exec_list_objects_v2),
    "TotalTimeVerifyAllKeysPerLambda": format_seconds(total_time_verify_all_keys)
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

def create_historical_recovery_key(content, bucket, historical_recovery_path, historical_path_prefix):
  response_file_key = content["Key"]
  file_name = response_file_key.rsplit('/', 1)[-2] #transactionID
  historical_recovery_key = historical_recovery_path + '/' + str(historical_path_prefix) + '/' + file_name + '.txt'
  logger.info('Generate historical recovery file {} in bucket {}'.format(historical_recovery_key, bucket))
  s3.put_object(Body=response_file_key.encode(), Bucket=bucket, Key=historical_recovery_key)

def read_and_validate_environment_vars():
  bucket_name = os.environ["BUCKET_NAME"]
  if not bucket_name:
    raise Exception("bucket_name is not defined")

  historical_recovery_path = os.environ["HISTORICAL_RECOVERY_PATH"]
  if not historical_recovery_path:
    raise Exception("historical_recovery_path is not defined")

  prefixes = os.environ["PREFIXES"]
  if not prefixes:
    raise Exception("prefixes are not defined")

  suffixes = os.environ["SUFFIXES"]
  if not suffixes:
    raise Exception("suffixes are not defined")

  s3_keys_listing_limit_per_call = os.environ["S3_KEYS_LISTING_LIMIT_PER_CALL"]
  if not s3_keys_listing_limit_per_call:
    raise Exception("s3_keys_listing_limit_per_call is not defined")

  s3_keys_listing_limit_per_call = os.environ["S3_KEYS_LISTING_LIMIT_PER_CALL"]
  if not s3_keys_listing_limit_per_call:
    raise Exception("s3_keys_listing_limit_per_call is not defined")

  max_keys_per_historical_path_prefix = os.environ["MAX_KEYS_PER_HISTORICAL_PATH_PREFIX"]
  if not max_keys_per_historical_path_prefix:
    raise Exception("max_keys_per_historical_path_prefix is not defined")
  if int(max_keys_per_historical_path_prefix) < 1:
    raise Exception("max_keys_per_historical_path_prefix must be more than zero")

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

  lambda_execution_limit_seconds = os.environ["LAMBDA_EXECUTION_LIMIT_SECONDS"]
  if not lambda_execution_limit_seconds:
    raise Exception("lambda_execution_limit_seconds is not defined")

  historical_recovery_path_metadata = os.environ["HISTORICAL_RECOVERY_PATH_METADATA"]

  environment_vars = {
    "bucket": bucket_name,
    "historical_recovery_path": historical_recovery_path,
    "prefixes": tuple(set(prefixes.split(","))),
    "suffixes": tuple(set(suffixes.split(","))),
    "last_modified_start_datetime": last_modified_start_datetime,
    "last_modified_end_datetime": last_modified_end_datetime,
    "s3_keys_listing_limit_per_call": int(s3_keys_listing_limit_per_call),
    "lambda_execution_limit_seconds": int(lambda_execution_limit_seconds),
    "historical_recovery_path_metadata": historical_recovery_path_metadata,
    "max_keys_per_historical_path_prefix": int(max_keys_per_historical_path_prefix)
  }
  return environment_vars

def format_seconds(seconds):
  if seconds <= 0:
    return "00 min 00 sec"
  else:
    return time.strftime("%H hours %M min %S sec", time.gmtime(seconds))

def verify_next_key_exists(bucket, prefix, last_verified_key):
  if not last_verified_key:
    return False
  kwargs = {'Bucket': bucket}
  kwargs['Prefix'] = prefix
  kwargs['MaxKeys'] = 1
  kwargs['StartAfter'] = last_verified_key

  resp = s3.list_objects_v2(**kwargs)
  if len(resp.get('Contents', [])) > 0:
    return True
  return False

def verify_stop_flow_execution_flag(bucket, historical_recovery_path_metadata, prefix):
  try:
    key = historical_recovery_path_metadata + "/" + prefix + "/" + "stop.flag"
    s3.get_object(Bucket=bucket, Key=key)
    logger.info('Stop flow execution for bucket {} and prefix {}'.format(bucket, prefix))
    return True
  except ClientError:
    return False

def create_historical_path_prefix(total_not_processed_success_keys_per_prefix, max_keys_per_historical_path_prefix):
  logger.info("total_not_processed_success_keys_per_prefix {} and max_keys_per_historical_path_prefix {}".format(total_not_processed_success_keys_per_prefix, max_keys_per_historical_path_prefix))
  historical_path_prefix = 1
  if total_not_processed_success_keys_per_prefix > max_keys_per_historical_path_prefix:
    value = total_not_processed_success_keys_per_prefix % max_keys_per_historical_path_prefix
    if value == 0:
      historical_path_prefix = int(total_not_processed_success_keys_per_prefix / max_keys_per_historical_path_prefix)
    else:
      historical_path_prefix = int(total_not_processed_success_keys_per_prefix / max_keys_per_historical_path_prefix) + 1
  return historical_path_prefix

def lambda_handler(event, context):
  datetime_lambda_start = datetime.now()

  action_process_prefix = "process_prefix"
  action_process_prefixes = "process_prefixes"

  json_str = json.dumps(event)
  logger.info("Lambda received event {} ".format(json_str))
  json_object = json.loads(json_str)

  environment_vars = read_and_validate_environment_vars()
  bucket = environment_vars["bucket"]
  historical_recovery_path = environment_vars["historical_recovery_path"]
  prefixes = environment_vars["prefixes"]
  suffixes = environment_vars["suffixes"]
  last_modified_start_datetime = environment_vars["last_modified_start_datetime"]
  last_modified_end_datetime = environment_vars["last_modified_end_datetime"]
  s3_keys_listing_limit_per_call = environment_vars["s3_keys_listing_limit_per_call"]
  lambda_execution_limit_seconds = environment_vars["lambda_execution_limit_seconds"]
  historical_recovery_path_metadata = environment_vars["historical_recovery_path_metadata"]
  max_keys_per_historical_path_prefix = environment_vars["max_keys_per_historical_path_prefix"]
  action = json_object['action']

  if action == action_process_prefixes:
    logger.info('Started to process historical data with configuration: '
                'bucket {} ; historical_recovery_path {} ; '
                'prefixes {} ; suffixes {} ; '
                'last_modified_start_datetime {} ; last_modified_end_datetime {} ; '
                's3_keys_listing_limit_per_call {} ; lambda_execution_limit_seconds {} ; '
                'historical_recovery_path_metadata {} ; max_keys_per_historical_path_prefix {}'
                .format(bucket, historical_recovery_path, prefixes, suffixes,
                        last_modified_start_datetime, last_modified_end_datetime,
                        s3_keys_listing_limit_per_call, lambda_execution_limit_seconds,
                        historical_recovery_path_metadata, max_keys_per_historical_path_prefix))

    for prefix in prefixes:
      payload = json.dumps({"action": action_process_prefix,
                            "lambdaAsyncNumber": 1,
                            "totalTimeFlowExecSeconds": 0,
                            "totalVerifiedKeysPerPrefix": 0,
                            "totalNotProcessedSuccessKeysPerPrefix": 0,
                            "metadata": {
                              "prefix": prefix,
                              "lastVerifiedKey": ""
                            }})
      lambda_client.invoke(
          FunctionName=context.function_name,
          InvocationType='Event',
          Payload=payload
      )
  elif action == action_process_prefix:
    lambda_async_number = json_object['lambdaAsyncNumber']
    prefix_metadata = json_object['metadata']
    prefix = prefix_metadata['prefix']
    process_after_key_name = prefix_metadata['lastVerifiedKey']
    total_time_flow_execution = json_object['totalTimeFlowExecSeconds']
    total_verified_keys_per_prefix = json_object['totalVerifiedKeysPerPrefix']
    total_not_processed_success_keys_per_prefix = json_object['totalNotProcessedSuccessKeysPerPrefix']

    stop_flow_execution = verify_stop_flow_execution_flag(bucket, historical_recovery_path_metadata, prefix)
    if stop_flow_execution:
      return "success"

    #sleep_test(10)
    result = verify_not_processed_success_s3_keys(bucket, prefix, suffixes, process_after_key_name, s3_keys_listing_limit_per_call,
                       last_modified_start_datetime, last_modified_end_datetime, datetime_lambda_start,
                       lambda_execution_limit_seconds, historical_recovery_path,
                       total_not_processed_success_keys_per_prefix, max_keys_per_historical_path_prefix)

    len_not_processed_keys = result["NotProcessedSuccessKeys"]
    total_verified_keys_per_lambda = result["TotalVerifiedKeysPerLambda"]
    total_time_exec_list_objects_v2 = result["TotalTimeExecListObjectsV2"]
    time_exec_list_objects_v2_per_call = result["TimeExecListObjectsV2PerCall"]
    total_time_verify_all_keys_per_lambda = result["TotalTimeVerifyAllKeysPerLambda"]
    last_verified_key = result["LastVerifiedKey"]
    next_key_exists = verify_next_key_exists(bucket, prefix, last_verified_key)
    msg_key_exists = "exists" if next_key_exists == True else "does not exist"
    logger.info("Verified that next key {} after last_verified_key {}".format(msg_key_exists, last_verified_key))

    datetime_lambda_end = datetime.now()
    lambda_time_execution = int((datetime_lambda_end - datetime_lambda_start).total_seconds())
    total_time_flow_execution = total_time_flow_execution + lambda_time_execution
    total_not_processed_success_keys_per_prefix = total_not_processed_success_keys_per_prefix + len_not_processed_keys
    total_verified_keys_per_prefix = total_verified_keys_per_prefix + total_verified_keys_per_lambda
    if next_key_exists:
      payload = json.dumps({"action": action_process_prefix,
                            "lambdaAsyncNumber": lambda_async_number + 1,
                            "totalTimeFlowExecSeconds": total_time_flow_execution,
                            "totalVerifiedKeysPerPrefix": total_verified_keys_per_prefix,
                            "totalNotProcessedSuccessKeysPerPrefix": total_not_processed_success_keys_per_prefix,
                            "metadata": {
                              "prefix": prefix,
                              "lastVerifiedKey": last_verified_key
                            }})
      lambda_client.invoke(
          FunctionName=context.function_name,
          InvocationType='Event',
          Payload=payload
      )

    statsPayload = json.dumps({"bucket": bucket,
                               "prefix": prefix,
                               "lambdaAsyncNumber": lambda_async_number,
                               "timeLambdaExecution": format_seconds(lambda_time_execution),
                               "totalTimeFlowExecution": format_seconds(total_time_flow_execution),
                               "verifiedKeyFromName": process_after_key_name,
                               "verifiedKeyToName": last_verified_key,
                               "totalVerifiedKeysPerLambda": total_verified_keys_per_lambda,
                               "foundNotProcessedSuccessKeysPerLambda": len_not_processed_keys,
                               "totalVerifiedKeysPerPrefix": total_verified_keys_per_prefix,
                               "foundNotProcessedSuccessKeysPerPrefix": total_not_processed_success_keys_per_prefix,
                               "totalTimeExecListObjectsV2": total_time_exec_list_objects_v2,
                               "timeExecListObjectsV2PerCall": time_exec_list_objects_v2_per_call,
                               "totalTimeVerifyAllKeysPerLambda": total_time_verify_all_keys_per_lambda
                               })
    logger.info('Asynchronous lambda execution finished {}'.format(statsPayload))

    if not next_key_exists:
      logger.info('Data processing finished for bucket {} and prefix {}'.format(bucket, prefix))

  return "success"

def sleep_test(seconds):
  logger.info("before sleep")
  time.sleep(seconds)
  logger.info("after sleep")
