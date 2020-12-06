resource "aws_lambda_function" "lambda_function" {
  role = "${aws_iam_role.iam_lambda_role.arn}"
  handler = "${var.handler}"
  runtime = "${var.runtime}"
  timeout = 120
  filename = "../lambda.py.zip"
  function_name = "${var.function_name}"
  source_code_hash = filebase64sha256("../lambda.py.zip")

  environment {
    variables = {
      BUCKET_NAME = "all-transactions"
      HISTORICAL_RECOVERY_PATH = "historical-recovery-path"
      PREFIXES = "us-east1"
      SUFFIXES = ".json"
      LAST_MODIFIED_START = "11/30/2020 00:00:00-0000"
      LAST_MODIFIED_END = "12/30/2020 00:00:00-0000"
      S3_KEYS_LISTING_LIMIT_PER_CALL = "4"
      LAMBDA_EXECUTION_LIMIT_SECONDS = "5"
      HISTORICAL_RECOVERY_PATH_METADATA = "historical-recovery-path-metadata"
    }
  }
}

resource "aws_lambda_function" "lambda_read_recovery_path" {
  role = "${aws_iam_role.iam_lambda_role.arn}"
  handler = "${var.lambda_read_recovery_path}"
  runtime = "${var.runtime}"
  filename = "../lambda.py.zip"
  timeout = 60
  function_name = "${var.lambda_read_recovery_path_function_name}"
  source_code_hash = filebase64sha256("../lambda.py.zip")

  environment {
    variables = {
      BUCKET_NAME = "all-transactions"
      HISTORICAL_RECOVERY_PATH = "historical-recovery-path"
      SCAN_DATE_START = "12-06-2020"
      SCAN_DATE_END = "12-06-2020"
      FETCH_MAX_S3_KEYS_PER_S3_LISTING_CALL = "2"
      LAMBDA_WORKING_LIMIT_SECONDS = "60"
    }
  }
}