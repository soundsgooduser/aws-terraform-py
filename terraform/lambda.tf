resource "aws_lambda_function" "lambda_function" {
  role = "${aws_iam_role.iam_lambda_role.arn}"
  handler = "${var.handler}"
  runtime = "${var.runtime}"
  filename = "../lambda.py.zip"
  function_name = "${var.function_name}"
  source_code_hash = filebase64sha256("../lambda.py.zip")

  environment {
    variables = {
      BUCKET_NAME = "all-transactions"
      HISTORICAL_RECOVERY_PATH = "historical-recovery-path"
      PREFIXES = "us-east1"
      SUFFIXES = ".json"
      LAST_MODIFIED_START = "11/01/2020 00:00:00-0000"
      LAST_MODIFIED_END = "11/30/2020 00:00:00-0000"
      DEFAULT_PROCESS_MAX_KEYS_PER_LAMBDA = "2"
      PROCESS_MAX_KEYS_PER_LAMBDA = "4"
    }
  }
}