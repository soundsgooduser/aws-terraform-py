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
athena_client = boto3.client('athena')

def verify_not_processed_success_s3_keys(bucket, keys, historical_recovery_path):
  verified_keys = 0
  not_processed_success_keys = 0

  datetime_verify_keys_start = datetime.now()

  for key in keys:
    verified_keys = verified_keys + 1
    key_exists = verify_key_processed_success_exists_in_s3(bucket, key)
    if not key_exists:
      create_historical_recovery_key(bucket, key, historical_recovery_path)
      not_processed_success_keys = not_processed_success_keys + 1

  datetime_verify_keys_end = datetime.now()
  total_time_verify_all_keys = int((datetime_verify_keys_end - datetime_verify_keys_start).total_seconds())

  result = {
    "VerifiedKeys": verified_keys,
    "NotProcessedSuccessKeys": not_processed_success_keys,
    "TotalTimeVerifyAllKeys": format_seconds(total_time_verify_all_keys)
  }
  return result

def verify_key_processed_success_exists_in_s3(bucket, key):
  data_response = s3.get_object(Bucket=bucket, Key=key)
  json_data_response = data_response['Body'].read().decode('utf-8')
  id = hashlib.md5(json_data_response.encode('utf-8')).hexdigest()
  processed_success_key = key.replace('/Response.json', '.' + id + '.ods.processed.success')
  try:
    #check if success marker file exists
    s3.get_object(Bucket=bucket, Key=processed_success_key)
    #exists, file processed
    logger.info('Exists processed success key {} for file {} in bucket {} '
                .format(processed_success_key, key, bucket))
    return True
  except ClientError:
    #does not exist, file not processed
    logger.info('Does not exist processed success key {} for file {} in bucket {} '
                .format(processed_success_key, key, bucket))
    return False

def create_historical_recovery_key(bucket, key, historical_recovery_path):
  logger.info("key>>>>>>>>> {}".format(key))
  file_name = key.rsplit('/', 1)[-2] #transactionID
  historical_recovery_key = historical_recovery_path + '/' + file_name + '.txt'
  logger.info('Generate historical recovery file {} in bucket {}'.format(historical_recovery_key, bucket))
  s3.put_object(Body=key.encode(), Bucket=bucket, Key=historical_recovery_key)

def read_and_validate_environment_vars():
  bucket_name = os.environ["BUCKET_NAME"]
  if not bucket_name:
    raise Exception("bucket_name is not defined")
  historical_recovery_path = os.environ["HISTORICAL_RECOVERY_PATH"]
  if not historical_recovery_path:
    raise Exception("historical_recovery_path is not defined")
  prefix = os.environ["PREFIX"]
  if not prefix:
    raise Exception("prefix is not defined")
  s3_keys_listing_limit_per_call = os.environ["S3_KEYS_LISTING_LIMIT_PER_CALL"]
  if not s3_keys_listing_limit_per_call:
    raise Exception("s3_keys_listing_limit_per_call is not defined")

  historical_recovery_path_metadata = os.environ["HISTORICAL_RECOVERY_PATH_METADATA"]

  environment_vars = {
    "bucket": bucket_name,
    "historical_recovery_path": historical_recovery_path,
    "prefix": prefix,
    "s3_keys_listing_limit_per_call": int(s3_keys_listing_limit_per_call),
    "historical_recovery_path_metadata": historical_recovery_path_metadata
  }
  return environment_vars

def format_seconds(seconds):
  if seconds <= 0:
    return "00 min 00 sec"
  else:
    return time.strftime("%H hours %M min %S sec", time.gmtime(seconds))

def results_to_df(results):

  columns = [
    col['Label']
    for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']
  ]

  listed_results = []
  for res in results['ResultSet']['Rows'][1:]:
    values = []
    for field in res['Data']:
      try:
        values.append(list(field.values())[0])
      except:
        values.append(' ')

    listed_results.append(
      dict(zip(columns, values))
    )

  return listed_results

def createKeys(prefix, res):
  keys = []
  for value in res:
    key = createKey(prefix, value)
    print("key >>> {}".format(key))
    if key:
      keys.append(key)
  return keys

def createKey(prefix, row):
  tenantId = row['tenantid']
  applicationId = row['applicationid']
  consumerId = row['consumerid']
  referenceId = row['referenceid']
  productOrchestrationId = row['productorchestrationid']
  transactionId = row['transactionid']
  if not tenantId or not applicationId or not consumerId or not referenceId or not productOrchestrationId or not transactionId:
    logger.info("Row not valid to create key: {} ".format(row))
    return ""

  return prefix + '/' + row['tenantid'] + '/' + row['applicationid'] + '/' + \
         row['consumerid'] + '/' + row['referenceid'] + '/response/' + row['productorchestrationid'] + \
         '/' + row['transactionid'] + '/' + 'Response.json'

def lambda_handler(event, context):
  datetime_lambda_start = datetime.now()

  json_str = json.dumps(event)
  logger.info("Lambda received event {} ".format(json_str))
  json_object = json.loads(json_str)

  query_execution_id = json_object['queryExecutionId']
  next_token = json_object['nextToken']
  lambda_async_number = json_object['lambdaAsyncNumber']
  total_time_flow_execution = json_object['totalTimeFlowExecSeconds']
  total_verified_keys_per_prefix = json_object['totalVerifiedKeysPerPrefix']
  total_not_processed_success_keys_per_prefix = json_object['totalNotProcessedSuccessKeysPerPrefix']

  environment_vars = read_and_validate_environment_vars()
  s3_keys_listing_limit_per_call = environment_vars["s3_keys_listing_limit_per_call"]
  historical_recovery_path = environment_vars["historical_recovery_path"]
  bucket = environment_vars["bucket"]
  prefix = environment_vars["prefix"]

  datetime_get_query_results_start = datetime.now()

  if next_token:
    response_query_result = athena_client.get_query_results(
      QueryExecutionId = query_execution_id,
      MaxResults = s3_keys_listing_limit_per_call,
      NextToken = next_token
    )
  else:
    response_query_result = athena_client.get_query_results(
      QueryExecutionId = query_execution_id,
      MaxResults = s3_keys_listing_limit_per_call
    )

  datetime_get_query_results_end = datetime.now()
  get_query_results_time_execution = int((datetime_get_query_results_end - datetime_get_query_results_start).total_seconds())

  print("res >>> {}".format(response_query_result))
  res = results_to_df(response_query_result)
  print("res >>> {}".format(res))

  keys = createKeys(prefix, res)
  print("KEYS >>> {}".format(keys))

  result = verify_not_processed_success_s3_keys(bucket, keys, historical_recovery_path)

  datetime_lambda_end = datetime.now()
  lambda_time_execution = int((datetime_lambda_end - datetime_lambda_start).total_seconds())
  total_time_flow_execution = total_time_flow_execution + lambda_time_execution
  total_verified_keys_per_lambda = result["VerifiedKeys"]
  len_not_processed_keys = result["NotProcessedSuccessKeys"]
  total_time_verify_all_keys_per_lambda = result["TotalTimeVerifyAllKeys"]
  total_not_processed_success_keys_per_prefix = total_not_processed_success_keys_per_prefix + len_not_processed_keys
  total_verified_keys_per_prefix = total_verified_keys_per_prefix + total_verified_keys_per_lambda


  statsPayload = json.dumps({"bucket": bucket,
                             "prefix": prefix,
                             "lambdaAsyncNumber": lambda_async_number,
                             "timeLambdaExecution": format_seconds(lambda_time_execution),
                             "totalTimeFlowExecution": format_seconds(total_time_flow_execution),
                             "totalVerifiedKeysPerLambda": total_verified_keys_per_lambda,
                             "foundNotProcessedSuccessKeysPerLambda": len_not_processed_keys,
                             "totalVerifiedKeysPerPrefix": total_verified_keys_per_prefix,
                             "foundNotProcessedSuccessKeysPerPrefix": total_not_processed_success_keys_per_prefix,
                             "totalTimeVerifyAllKeysPerLambda": total_time_verify_all_keys_per_lambda,
                             "getQueryResultsTimeExecution": get_query_results_time_execution
                             })

  logger.info('Asynchronous lambda execution finished {}'.format(statsPayload))

  try:
    next_token = response_query_result['NextToken']
  except KeyError:
    next_token = ""

  if next_token:
    payload = json.dumps({
      "queryExecutionId": query_execution_id,
      "nextToken": next_token,
      "lambdaAsyncNumber": lambda_async_number + 1,
      "totalTimeFlowExecSeconds": total_time_flow_execution,
      "totalVerifiedKeysPerPrefix": total_verified_keys_per_prefix,
      "totalNotProcessedSuccessKeysPerPrefix": total_not_processed_success_keys_per_prefix
    })
    lambda_client.invoke(
      FunctionName=context.function_name,
      InvocationType='Event',
      Payload=payload
    )
  else:
    logger.info('Data processing finished for bucket {} and prefix {}'.format(bucket, prefix))

  return "success"

def sleep_test(seconds):
  logger.info("before sleep")
  time.sleep(seconds)
  logger.info("after sleep")
