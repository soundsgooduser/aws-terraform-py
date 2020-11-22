import json
import boto3
import datetime
from datetime import timedelta


def handler(event, context):
  bucket = "all-transactions"  # take from ENV VARS
  action_process_prefix = "process_prefix"
  action_process_prefixes = "process_prefixes"

  json_str = json.dumps(event)
  print("Received event: " + json_str)
  json_object = json.loads(json_str)

  action = json_object['action']
  if action == action_process_prefixes:
    print(action_process_prefixes)
    prefixes = ("test1", "test2", "test3")  # take from ENV VARS
    count_prefixes = len(prefixes)
    count_concurrent_lambdas = int(9);  # take from ENV VARS
    date_time_range_hours = int(24)  # take from ENV VARS
    time_ranges = calculate_time_ranges(count_concurrent_lambdas,
                                        count_prefixes, date_time_range_hours)
    print(time_ranges)
    lambda_client = boto3.client('lambda')
    for prefix in prefixes:
      for x in range(len(time_ranges) - 1):
        payload = json.dumps({"action": action_process_prefix,
                              "metadata": {
                                "prefixName": prefix,
                                "startDateTime": time_ranges[x],
                                "endDateTime": time_ranges[x + 1]
                              }})
        lambda_client.invoke(
            FunctionName="minimal_lambda_function",
            InvocationType='Event',
            Payload=payload
        )
  elif action == action_process_prefix:
    print(action_process_prefix)
    prefix_metadata = json_object['metadata']
    prefix_name = prefix_metadata['prefixName']
    start_date_time = prefix_metadata['startDateTime']
    end_date_time = prefix_metadata['endDateTime']
    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket}
    kwargs['Prefix'] = prefix_name
    resp = s3.list_objects_v2(**kwargs)
    for content in resp.get('Contents', []):
      print(content)
      last_modified = content['LastModified']
      # if last_modified is beetwen start_date_time and end_date_time then we need to process it
      # call S3 to get Body
      # create id by encrypting body
      # verify if file has been processed successfully and if not save it to S3 for reprocessing
  else:
    print("action is no valid: " + action)


def calculate_time_ranges(count_concurrent_lambdas, count_prefixes,
    date_time_range_hours):
  count_lambdas_per_prefix = int(count_concurrent_lambdas / count_prefixes)
  delta_hours = int(date_time_range_hours / count_lambdas_per_prefix)
  time_ranges = [];
  for index in range(0, count_prefixes + 1):
    if index == 0:
      time_ranges.append(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    else:
      time_ranges.append((datetime.datetime.now() + timedelta(
        hours=-(delta_hours * index))).strftime("%Y-%m-%d %H:%M:%S"))
  time_ranges.reverse()
  return time_ranges
