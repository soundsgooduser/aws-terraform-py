import os
import logging
import boto3
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')
sqs_client = boto3.client('sqs')

def process_prefix_async_call(action, lambda_async_number, prefix, last_verified_key,
    function_name):
  payload = json.dumps({"action": action,
                        "lambdaAsyncNumber": lambda_async_number,
                        "prefix": prefix,
                        "lastVerifiedKey": last_verified_key
                        })
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

  fetch_max_s3_keys_per_one_s3_call = os.environ["FETCH_MAX_S3_KEYS_PER_ONE_S3_CALL"]
  if not fetch_max_s3_keys_per_one_s3_call:
    raise Exception("fetch_max_s3_keys_per_one_s3_call is not defined")

  max_s3_calls_per_lambda = os.environ["MAX_S3_CALLS_PER_LAMBDA"]
  if not max_s3_calls_per_lambda:
    raise Exception("max_s3_calls_per_lambda is not defined")

  environment_vars = {
    "bucket": bucket,
    "prefixes": tuple(set(prefixes.split(","))),
    "sqs_keys_queue_url": sqs_keys_queue_url,
    "fetch_max_s3_keys_per_one_s3_call": int(fetch_max_s3_keys_per_one_s3_call),
    "max_s3_calls_per_lambda": int(max_s3_calls_per_lambda)
  }
  return environment_vars

def lambda_handler(event, context):
  action_process_prefix = "process_prefix"
  action_process_prefixes = "process_prefixes"

  json_str = json.dumps(event)
  logger.info("Lambda received event {} ".format(json_str))
  json_object = json.loads(json_str)

  environment_vars = read_and_validate_environment_vars()
  bucket = environment_vars["bucket"]
  prefixes = environment_vars["prefixes"]
  fetch_max_s3_keys_per_one_s3_call = environment_vars["fetch_max_s3_keys_per_one_s3_call"]
  max_s3_calls_per_lambda = environment_vars["max_s3_calls_per_lambda"]
  sqs_keys_queue_url = environment_vars["sqs_keys_queue_url"]
  action = json_object['action']

  if action == action_process_prefixes:
    logger.info('Started to process historical data with configuration: '
                'bucket {} ; prefixes {} ; '
                'fetch_max_s3_keys_per_one_s3_call {} ; max_s3_calls_per_lambda {}'
                .format(bucket, prefixes, fetch_max_s3_keys_per_one_s3_call, max_s3_calls_per_lambda))

    for prefix in prefixes:
      process_prefix_async_call(action_process_prefix, 1, prefix, "", context.function_name)

  elif action == action_process_prefix:
    lambda_async_number = json_object['lambdaAsyncNumber']
    prefix = json_object['prefix']
    start_after_key = json_object['lastVerifiedKey']

    kwargs = {'Bucket': bucket}
    kwargs['Prefix'] = prefix
    kwargs['MaxKeys'] = fetch_max_s3_keys_per_one_s3_call
    if start_after_key:
      kwargs['StartAfter'] = start_after_key

    last_verified_key = ""
    for iteration in range(0, max_s3_calls_per_lambda):
      s3_response = s3_client.list_objects_v2(**kwargs)
      s3_contents = s3_response.get('Contents', [])
      len_s3_contents = len(s3_contents)
      if len_s3_contents == 0:
        logger.info("No keys to process returned from S3.")
        break
      else:
        verify_to_key = s3_contents[len_s3_contents - 1]['Key']
        logger.info("Iteration {} ".format(iteration))
        sqs_msg_payload = json.dumps({"bucket": bucket,
                                      "prefix": prefix,
                                      "verifyFromKey": start_after_key if iteration == 0 else last_verified_key,
                                      "verifyToKey": verify_to_key
                                     }
                                    )
        last_verified_key = verify_to_key
        kwargs['StartAfter'] = last_verified_key

        logger.info("Request to SQS {} ".format(sqs_msg_payload))
        response = sqs_client.send_message(QueueUrl=sqs_keys_queue_url,MessageBody=sqs_msg_payload)
        logger.info("Response from SQS {} ".format(response))
    # TODO: verify if key after last_verified_key exists
    if last_verified_key:
      process_prefix_async_call(action_process_prefix, lambda_async_number + 1, prefix, last_verified_key, context.function_name)

  return "success"
