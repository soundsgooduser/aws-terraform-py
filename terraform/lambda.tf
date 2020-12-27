resource "aws_lambda_function" "lambda_sender" {
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
      HISTORICAL_RECOVERY_PATH_METADATA = "historical-recovery-path-metadata"
      FETCH_MAX_S3_KEYS_PER_S3_LISTING_CALL = "100"
      LAMBDA_WORKING_LIMIT_SECONDS = "40"
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
  timeout = 60

  environment {
    variables = {
      HISTORICAL_RECOVERY_PATH = "historical-recovery-path"
      FETCH_MAX_S3_KEYS_PER_S3_LISTING_CALL = "100"
      LAMBDA_WORKING_LIMIT_SECONDS = "40"
    }
  }
}

resource "aws_lambda_function" "lambda_recovery" {
  role = "${aws_iam_role.iam_lambda_role.arn}"
  handler = "${var.lambda_recovery_handler}"
  runtime = "${var.runtime}"
  filename = "../lambda.py.zip"
  function_name = "${var.s3_keys_lambda_recovery_name}"
  source_code_hash = filebase64sha256("../lambda.py.zip")
  timeout = 60

  environment {
    variables = {
      BUCKET_NAME = "all-transactions"
      HISTORICAL_RECOVERY_PATH = "historical-recovery-path"
      FETCH_MAX_S3_KEYS_PER_S3_LISTING_CALL = "100"
      LAMBDA_WORKING_LIMIT_SECONDS = "40"
    }
  }
}