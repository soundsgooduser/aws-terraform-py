resource "aws_lambda_function" "lambda_function" {
  role = "${aws_iam_role.iam_lambda_role.arn}"
  handler = "${var.lambda_sender_handler}"
  runtime = "${var.runtime}"
  timeout = 60
  filename = "../lambda.py.zip"
  function_name = "${var.s3_keys_lambda_sender_name}"
  source_code_hash = filebase64sha256("../lambda.py.zip")

  environment {
    variables = {
      BUCKET = "all-transactions"
      PREFIXES = "us-east1"
      FETCH_MAX_S3_KEYS_PER_S3_LISTING_CALL = "4"
      LAMBDA_WORKING_LIMIT_SECONDS = "4"
      SQS_KEYS_QUEUE_URL = "${aws_sqs_queue.s3-keys-queue.id}"
    }
  }
}


resource "aws_lambda_function" "lambda_receiver" {
  role = "${aws_iam_role.iam_lambda_role.arn}"
  handler = "${var.lambda_receiver_handler}"
  runtime = "${var.runtime}"
  filename = "../lambda.py.zip"
  function_name = "${var.s3_keys_lambda_receiver_name}"
  source_code_hash = filebase64sha256("../lambda.py.zip")

  environment {
    variables = {
      HISTORICAL_RECOVERY_PATH = "historical-recovery-path"
      SUFFIXES = "Response.json"
      FETCH_MAX_S3_KEYS_PER_S3_LISTING_CALL = "4"
      LAST_MODIFIED_START = "11/01/2020 00:00:00-0000"
      LAST_MODIFIED_END = "12/30/2020 00:00:00-0000"
      SAVE_RESULT_TO_S3 = "yes" # yes/no
    }
  }
}