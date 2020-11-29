import os
import logging
import boto3
import hashlib
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
  json_str = json.dumps(event)
  logger.info("Lambda received event {} ".format(json_str))
  for record in event['Records']:
    payload = record["body"]
    logger.info("Payload {} ".format(str(payload)))
