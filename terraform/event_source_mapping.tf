resource "aws_lambda_event_source_mapping" "event_source_mapping_s3_keys_queue_to_lambda_receiver" {
  event_source_arn = aws_sqs_queue.s3-keys-queue.arn
  function_name    = aws_lambda_function.lambda_receiver.arn
}