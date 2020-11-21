resource "aws_lambda_function" "lambda_function" {
  role = "${aws_iam_role.iam_lambda_role.arn}"
  handler = "${var.handler}"
  runtime = "${var.runtime}"
  filename = "../lambda.py.zip"
  function_name = "${var.function_name}"
  source_code_hash = filebase64sha256("../lambda.py.zip")
}