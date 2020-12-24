resource "aws_sqs_queue" "s3-keys-queue" {
  name = "s3-keys-queue"
  visibility_timeout_seconds = 300
}