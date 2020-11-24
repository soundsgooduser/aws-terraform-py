import os
import logging
import boto3
import hashlib
import dateutil.parser
import pytz
import argparse

from botocore.exceptions import ClientError
from collections import namedtuple

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
client = boto3.resource('s3')

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


def get_s3_objects(bucket, prefixes=None, suffixes=None, last_modified_min=None, last_modified_max=None):
    """
    Generate the objects in an S3 bucket. Adapted from:
    https://alexwlchan.net/2017/07/listing-s3-keys/

    :param bucket: Name of the S3 bucket.
    :ptype bucket: str
    :param prefixes: Only fetch keys that start with these prefixes (optional).
    :ptype prefixes: tuple
    :param suffixes: Only fetch keys that end with thes suffixes (optional).
    :ptype suffixes: tuple
    :param last_modified_min: Only yield objects with LastModified dates greater than this value (optional).
    :ptype last_modified_min: datetime.date
    :param last_modified_max: Only yield objects with LastModified dates greater than this value (optional).
    :ptype last_modified_max: datetime.date

    :returns: generator of dictionary objects
    :rtype: dict https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects
    """
    if last_modified_min and last_modified_max and last_modified_max < last_modified_min:
        raise ValueError(
            "When using both, last_modified_max: {} must be greater than last_modified_min: {}".format(
                last_modified_max, last_modified_min
            )
        )
    # Use the last_modified_rules dict to lookup which conditional logic to apply
    # based on which arguments were supplied
    last_modified_rule = last_modified_rules[bool(last_modified_min), bool(last_modified_max)]

    if not prefixes:
        prefixes = ('',)
    else:
        prefixes = tuple(set(prefixes))
    if not suffixes:
        suffixes = ('',)
    else:
        suffixes = tuple(set(suffixes))

    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket}

    for prefix in prefixes:
        kwargs['Prefix'] = prefix
        while True:
            # The S3 API response is a large blob of metadata.
            # 'Contents' contains information about the listed objects.
            resp = s3.list_objects_v2(**kwargs)
            for content in resp.get('Contents', []):
                last_modified_date = content['LastModified']
                if (
                        content['Key'].endswith(suffixes) and
                        last_modified_rule(last_modified_min, last_modified_date, last_modified_max) and
                        is_not_processed(bucket, content)
                ):
                    yield content

            # The S3 API is paginated, returning up to 1000 keys at a time.
            # Pass the continuation token into the next response, until we
            # reach the final page (when this field is missing).
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

def get_s3_keys(bucket, prefixes=None, suffixes=None, last_modified_min=None, last_modified_max=None):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :ptype bucket: str
    :param prefixes: Only fetch keys that start with these prefixes (optional).
    :ptype prefixes: tuple
    :param suffixes: Only fetch keys that end with thes suffixes (optional).
    :ptype suffixes: tuple
    :param last_modified_min: Only yield objects with LastModified dates greater than this value (optional).
    :ptype last_modified_min: datetime.date
    :param last_modified_max: Only yield objects with LastModified dates greater than this value (optional).
    :ptype last_modified_max: datetime.date
    """
    for obj in get_s3_objects(bucket, prefixes, suffixes, last_modified_min, last_modified_max):
        yield obj['Key']

def valid_datetime(date):
    if date is None:
        return date
    try:
        utc = pytz.UTC
        return utc.localize(dateutil.parser.parse(date))
    except Exception:
        raise argparse.ArgumentTypeError("Could not parse value: '{}' to type datetime".format(date))

def is_not_processed(bucket_name, content):
    file_key = content['Key']
    #get md5
    data_response = s3.get_object(Bucket=bucket_name, Key=file_key)
    json_data_response = data_response['Body'].read().decode('utf-8')
    id = hashlib.md5(json_data_response.encode('utf-8')).hexdigest()
    try:
        #check if success file exists
        s3.get_object(Bucket=bucket_name, Key=file_key.replace('json', id + 'processed.success'))
        #exists, file processed
        return False
    except ClientError:
        #does not exist, file not processed
        return True

def lambda_handler(event, context):
    bucket_name = os.environ["BUCKET_NAME"]
    prefixes = os.environ["PREFIXES"]
    suffixes = os.environ["SUFFIXES"]
    last_modified_start = os.environ["START_DATE-TIME"]
    last_modified_end = os.environ["END_DATE_TIME"]
    rec_path = os.environ["REC_PATH"]

    keys = get_s3_keys(bucket_name, prefixes, suffixes, last_modified_start, last_modified_end)
    for response_file_key in keys:
        logger.info('Reading {} from {}'.format(response_file_key, bucket_name))
        #RAE
        file_name = response_file_key.rsplit('/', 1)[-1].replace('.json','')
        #DocV
        #file_name = response_file_key.rsplit('/', 1)[-2] #transactionID
        logger.info('Generate recovery file {} in {}'.format(rec_path + file_name + '.txt', bucket_name))
        s3.put_object(Body=response_file_key.encode(), Bucket=bucket_name, Key=rec_path + file_name + '.txt')
        #call processing lambda here
    return