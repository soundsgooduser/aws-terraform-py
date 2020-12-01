# build and deploy to AWS

sh build.sh
cd terraform
terraform init
terraform plan
terraform apply
terraform destroy


aws cloudwatch get-metric-statistics --metric-name NumberOfObjects --namespace AWS/S3 --start-time 2020-11-25T00:00:00Z --end-time 2020-11-26T00:00:00Z --statistics Sum --region us-east-1 --dimensions Name=BucketName,Value=all-transactions Name=StorageType,Value=AllStorageTypes --period 86400 --output json

lambda_read_recovery_path.py

keys 1:
contents 0
start date < end date    run assync (lvk) changing date

contents 0
start date == end date   finish

contents len < max keys
start date < end date.   run assync (lvk) changing date

contents len < max keys
start date == end date   finish

contents 1000
start date < end date    run assync (lvk) not changing date

contents 1000
start date == end date   run assync (lvk) not changing date



============

timeLambdaExecution
totalTimeFlowExecution

totalVerifiedKeysPerLambda
totalVerifiedKeysPerFlow
totalFoundNotProcessedSuccessKeysPerLambda
totalFoundNotProcessedSuccessKeysPerFlow

timeS3ListingPerLambda{
	"1 listing call": ""
	"2 listing call": ""
}
totalTimeS3ListingPerLambda