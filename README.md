# build and deploy to AWS

sh build.sh
cd terraform
terraform init
terraform plan
terraform apply
terraform destroy


aws cloudwatch get-metric-statistics --metric-name NumberOfObjects --namespace AWS/S3 --start-time 2020-11-25T00:00:00Z --end-time 2020-11-26T00:00:00Z --statistics Sum --region us-east-1 --dimensions Name=BucketName,Value=all-transactions Name=StorageType,Value=AllStorageTypes --period 86400 --output json


test:

start sender request:
{
  "action": "start_processing",
  "prefix": "rules",
  "startProcessingDate": "2020-11-22",
  "endProcessingDate": "2020-11-22"
}

start recovery:

{
  "action": "process_prefixes",
  "prefixEventType": "rules",
  "prefixDate": "2020-11-22",
  "startPrefixNumber": "14",
  "endPrefixNumber": "14"
}

{
  "action": "process_prefixes",
  "prefixEventType": "rules",
  "prefixDate": "2020-11-22"
}