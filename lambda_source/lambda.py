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

def get_s3_objects_copy(bucket, prefix, suffixes, process_after_key_name, process_max_keys_per_lambda, last_modified_start_datetime, last_modified_end_datetime):
  # Use the last_modified_rules dict to lookup which conditional logic to apply
  # based on which arguments were supplied
  last_modified_rule = last_modified_rules[bool(last_modified_start_datetime), bool(last_modified_end_datetime)]

  kwargs = {'Bucket': bucket}
  kwargs['Prefix'] = prefix
  kwargs['MaxKeys'] = process_max_keys_per_lambda
  if process_after_key_name:
    kwargs['StartAfter'] = process_after_key_name

  logger.info("Start to process prefix {} in bucket {} after key name {}".format(prefix, bucket, process_after_key_name))
  resp = s3.list_objects_v2(**kwargs)
  for content in resp.get('Contents', []):
    file_key = content['Key']
    last_modified_datetime = content['LastModified']
    logger.info("Start to process file_key {} with last_modified_datetime {}".format(file_key, last_modified_datetime))
    if (file_key.endswith(suffixes) and
        last_modified_rule(last_modified_start_datetime, last_modified_datetime, last_modified_end_datetime) and
        key_is_not_processed(bucket, file_key)
    ):
      yield file_key

def get_s3_objects(bucket, prefix, suffixes, process_after_key_name, default_process_max_keys_per_lambda,
                   process_max_keys_per_lambda, last_modified_start_datetime, last_modified_end_datetime):
  # Use the last_modified_rules dict to lookup which conditional logic to apply
  # based on which arguments were supplied
  last_modified_rule = last_modified_rules[bool(last_modified_start_datetime), bool(last_modified_end_datetime)]

  kwargs = {'Bucket': bucket}
  kwargs['Prefix'] = prefix
  if process_after_key_name:
    kwargs['StartAfter'] = process_after_key_name

  logger.info("Start to process prefix {} in bucket {} after key name {}".format(prefix, bucket, process_after_key_name))

  count_iteration = calculate_count_iteration(default_process_max_keys_per_lambda, process_max_keys_per_lambda)
  logger.info("Calculated count iterations {}".format(count_iteration))

  remain_process_keys = process_max_keys_per_lambda
  for x in range(0, count_iteration):
    if(process_max_keys_per_lambda > default_process_max_keys_per_lambda and remain_process_keys > default_process_max_keys_per_lambda):
      remain_process_keys = remain_process_keys - default_process_max_keys_per_lambda
      kwargs['MaxKeys'] = default_process_max_keys_per_lambda
      logger.info("Default MaxKeys is used {}".format(default_process_max_keys_per_lambda))
    else:
      kwargs['MaxKeys'] = remain_process_keys
      logger.info("Remain MaxKeys is used {}".format(remain_process_keys))
    resp = s3.list_objects_v2(**kwargs)
    last_key = ""
    for content in resp.get('Contents', []):
      file_key = content['Key']
      last_key = file_key
      last_modified_datetime = content['LastModified']
      if (file_key.endswith(suffixes) and
          last_modified_rule(last_modified_start_datetime, last_modified_datetime, last_modified_end_datetime) and
          key_is_not_processed(bucket, file_key)
      ):
        yield file_key
    kwargs['StartAfter'] = last_key

def get_s3_keys(bucket, prefix, suffixes, process_after_key_name, default_process_max_keys_per_lambda,
                process_max_keys_per_lambda, last_modified_start_datetime, last_modified_end_datetime):
  for file_key in get_s3_objects(bucket, prefix, suffixes, process_after_key_name, default_process_max_keys_per_lambda,
                                 process_max_keys_per_lambda, last_modified_start_datetime, last_modified_end_datetime):
    yield file_key

def key_is_not_processed(bucket, file_key):
  data_response = s3.get_object(Bucket=bucket, Key=file_key)
  json_data_response = data_response['Body'].read().decode('utf-8')
  id = hashlib.md5(json_data_response.encode('utf-8')).hexdigest()
  processed_key = file_key.replace('json', id + '.ods.processed.success')
  try:
    #check if success marker file exists
    s3.get_object(Bucket=bucket, Key=processed_key)
    #exists, file processed
    logger.info('Processed key {} exists for file {} in bucket {} '
                .format(processed_key, file_key, bucket))
    return False
  except ClientError:
    #does not exist, file not processed
    logger.info('Processed key {} does not exist for file {} in bucket {} '
                .format(processed_key, file_key, bucket))
    return True

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
  process_max_keys_per_lambda = os.environ["PROCESS_MAX_KEYS_PER_LAMBDA"]
  if not process_max_keys_per_lambda:
    raise Exception("process_max_keys_per_lambda is not defined")
  default_process_max_keys_per_lambda = os.environ["DEFAULT_PROCESS_MAX_KEYS_PER_LAMBDA"]
  if not default_process_max_keys_per_lambda:
    raise Exception("default_process_max_keys_per_lambda is not defined")

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
    "bucket": bucket_name,
    "historical_recovery_path": historical_recovery_path,
    "prefixes": tuple(set(prefixes.split(","))),
    "suffixes": tuple(set(suffixes.split(","))),
    "last_modified_start_datetime": last_modified_start_datetime,
    "last_modified_end_datetime": last_modified_end_datetime,
    "default_process_max_keys_per_lambda": int(default_process_max_keys_per_lambda),
    "process_max_keys_per_lambda": int(process_max_keys_per_lambda)
  }
  return environment_vars

def calculate_count_iteration(default_max_keys_per_lambda, process_max_keys_per_lambda):
  count_iteration = 1
  if process_max_keys_per_lambda > default_max_keys_per_lambda:
    if process_max_keys_per_lambda % default_max_keys_per_lambda == 0:
      count_iteration = int(process_max_keys_per_lambda/default_max_keys_per_lambda)
    else:
      count_iteration = int(process_max_keys_per_lambda/default_max_keys_per_lambda) + 1
  return count_iteration

def verify_next_key_exists(bucket, prefix, process_after_key_name):
  kwargs = {'Bucket': bucket}
  kwargs['Prefix'] = prefix
  kwargs['MaxKeys'] = 1
  kwargs['StartAfter'] = process_after_key_name

  logger.info("Verify if next key exist in bucket {} and prefix {} after key name {}".format(bucket, prefix, process_after_key_name))

  resp = s3.list_objects_v2(**kwargs)
  for content in resp.get('Contents', []):
    return 1
  return 0

def lambda_handler(event, context):
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
  default_process_max_keys_per_lambda = environment_vars["default_process_max_keys_per_lambda"]
  process_max_keys_per_lambda = environment_vars["process_max_keys_per_lambda"]
  action = json_object['action']
  if action == action_process_prefixes:
    logger.info('Started to process historical data with configuration: '
                'bucket_name {} ; historical_recovery_path {} ; '
                'prefixes {} ; suffixes {} ; '
                'last_modified_start_datetime {} ; last_modified_end_datetime {}'
                .format(bucket, historical_recovery_path, prefixes, suffixes,
                        last_modified_start_datetime, last_modified_end_datetime))

    for prefix in prefixes:
        payload = json.dumps({"action": action_process_prefix,
                              "metadata": {
                                "prefix": prefix,
                                "processAfterKeyName": ""
                              }})
        lambda_client.invoke(
            FunctionName=context.function_name,
            InvocationType='Event',
            Payload=payload
        )
  elif action == action_process_prefix:
    logger.info("Started action {}".format(action_process_prefix))
    prefix_metadata = json_object['metadata']
    prefix = prefix_metadata['prefix']
    process_after_key_name = prefix_metadata['processAfterKeyName']

    last_processed_key = ""
    keys = get_s3_keys(bucket, prefix, suffixes, process_after_key_name, default_process_max_keys_per_lambda,
                       process_max_keys_per_lambda, last_modified_start_datetime, last_modified_end_datetime)
    for response_file_key in keys:
      file_name = response_file_key.rsplit('/', 1)[-2] #transactionID
      historical_recovery_key = historical_recovery_path + '/' + file_name + '.txt'
      logger.info('Generate historical recovery file {} in bucket {}'.format(historical_recovery_key, bucket))
      s3.put_object(Body=response_file_key.encode(), Bucket=bucket, Key=historical_recovery_key)
      last_processed_key = response_file_key
    logger.info('Last processed key {}'.format(last_processed_key))
    next_key_exists = verify_next_key_exists(bucket, prefix, last_processed_key)
    logger.info('Next key exists {}'.format(next_key_exists))
    if next_key_exists == 1:
      payload = json.dumps({"action": action_process_prefix,
                            "metadata": {
                              "prefix": prefix,
                              "processAfterKeyName": last_processed_key
                          }})
      lambda_client.invoke(
          FunctionName=context.function_name,
          InvocationType='Event',
          Payload=payload
      )
      logger.info('Asynchronous call to process next keys with payload {}'.format(payload))
    else:
      logger.info('Historical data processing finished')
  return
