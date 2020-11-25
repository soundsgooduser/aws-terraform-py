import os
import logging
import boto3
import hashlib

from botocore.exceptions import ClientError
from collections import namedtuple
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')

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

def get_s3_objects(bucket, prefixes, suffixes, last_modified_start_datetime, last_modified_end_datetime):
  # Use the last_modified_rules dict to lookup which conditional logic to apply
  # based on which arguments were supplied
  last_modified_rule = last_modified_rules[bool(last_modified_start_datetime), bool(last_modified_end_datetime)]

  kwargs = {'Bucket': bucket}

  for prefix in prefixes:
    kwargs['Prefix'] = prefix
    logger.info("Start to process prefix {} in bucket {} ".format(prefix, bucket))
    while True:
      # The S3 API response is a large blob of metadata.
      # 'Contents' contains information about the listed objects.
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

      # The S3 API is paginated, returning up to 1000 keys at a time.
      # Pass the continuation token into the next response, until we
      # reach the final page (when this field is missing).
      try:
        kwargs['ContinuationToken'] = resp['NextContinuationToken']
      except KeyError:
        break

def get_s3_keys(bucket, prefixes, suffixes, last_modified_start_datetime, last_modified_end_datetime):
  for file_key in get_s3_objects(bucket, prefixes, suffixes, last_modified_start_datetime, last_modified_end_datetime):
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
    "bucket_name": bucket_name,
    "historical_recovery_path": historical_recovery_path,
    "prefixes": tuple(set(prefixes.split(","))),
    "suffixes": tuple(set(suffixes.split(","))),
    "last_modified_start_datetime": last_modified_start_datetime,
    "last_modified_end_datetime": last_modified_end_datetime
  }
  return environment_vars

def lambda_handler(event, context):
  environment_vars = read_and_validate_environment_vars()
  bucket_name = environment_vars["bucket_name"]
  historical_recovery_path = environment_vars["historical_recovery_path"]
  prefixes = environment_vars["prefixes"]
  suffixes = environment_vars["suffixes"]
  last_modified_start_datetime = environment_vars["last_modified_start_datetime"]
  last_modified_end_datetime = environment_vars["last_modified_end_datetime"]

  logger.info('Started to process historical data with configuration: '
              'bucket_name {} ; historical_recovery_path {} ; '
              'prefixes {} ; suffixes {} ; '
              'last_modified_start_datetime {} ; last_modified_end_datetime {}'
              .format(bucket_name, historical_recovery_path, prefixes, suffixes,
                      last_modified_start_datetime, last_modified_end_datetime))

  keys = get_s3_keys(bucket_name, prefixes, suffixes, last_modified_start_datetime, last_modified_end_datetime)
  for response_file_key in keys:
    file_name = response_file_key.rsplit('/', 1)[-2] #transactionID
    historical_recovery_key = historical_recovery_path + '/' + file_name + '.txt'
    logger.info('Generate historical recovery file {} in bucket {}'.format(historical_recovery_key, bucket_name))
    s3.put_object(Body=response_file_key.encode(), Bucket=bucket_name, Key=historical_recovery_key)
    #call processing lambda here
  return
