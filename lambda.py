import json
import boto3


def handler(event, context):
  json_str = json.dumps(event)
  print("Received event: " + json_str)
  json_object = json.loads(json_str)
  flag = json_object['flag']
  print(flag)
  run_async = "run_async"
  if flag == run_async:
    print("run lambda async")
    payload3 = b"""{
      "flag": "not_async"
    }"""

    client = boto3.client('lambda')
    client.invoke(
        FunctionName="minimal_lambda_function",
        InvocationType='Event',
        Payload=payload3
    )
  else:
    print("end")
