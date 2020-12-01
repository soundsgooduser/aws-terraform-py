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
lambda_client = boto3.client('lambda')

def format_seconds(seconds):
  if seconds <= 0:
    return "00 min 00 sec"
  else:
    return time.strftime("%M min %S sec", time.gmtime(seconds))

def lambda_handler(event, context):
  datetime_lambda_start = datetime.now()

  action_process_prefix = "process_prefix"
  action_process_prefixes = "process_prefixes"

  json_str = json.dumps(event)
  logger.info("Lambda received event {} ".format(json_str))
  json_object = json.loads(json_str)

  action = json_object['action']
  if action == action_process_prefixes:
    payload = json.dumps({"action": action_process_prefix,
                          "lambdaAsyncNumber": 1,
                          "totalTimeFlowExecSeconds": 0,
                          "prefix": "us-east-1"
                          })
    lambda_client.invoke(
        FunctionName=context.function_name,
        InvocationType='Event',
        Payload=payload
    )
  elif action == action_process_prefix:
    logger.info("process prefix")

    total_time_flow_execution = json_object['totalTimeFlowExecSeconds']
    lambda_async_number = json_object['lambdaAsyncNumber']
    prefix = json_object['prefix']

    if lambda_async_number == 4:
      return

    logger.info("before sleep")
    time.sleep(80)
    logger.info("after sleep")

    datetime_lambda_end = datetime.now()
    lambda_time_execution = int((datetime_lambda_end - datetime_lambda_start).total_seconds())
    total_time_flow_execution = total_time_flow_execution + lambda_time_execution

    payload = json.dumps({"action": action_process_prefix,
                          "lambdaAsyncNumber": lambda_async_number + 1,
                          "totalTimeFlowExecSeconds": total_time_flow_execution,
                          "prefix": prefix
                          })
    lambda_client.invoke(
        FunctionName=context.function_name,
        InvocationType='Event',
        Payload=payload
    )

    formatted_lambda_time_execution = format_seconds(lambda_time_execution)
    formatted_total_time_flow_execution = format_seconds(total_time_flow_execution)
    logger.info("lambda time execution stats: lambda_time_execution {} ; total_time_flow_execution {}"
                .format(formatted_lambda_time_execution, formatted_total_time_flow_execution))

  return "success"
