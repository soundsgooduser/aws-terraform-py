resource "aws_iam_role" "iam_lambda_role" {
  name = "py_aws_iam_role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow"
    }
  ]
}
EOF
}

resource "aws_iam_policy" "LambdaInline" {
  name = "py_aws_iam_role_policy"
  path = "/"
  description = "Lambda Inline Policy"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:CreateNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DetachNetworkInterface",
        "ec2:DeleteNetworkInterface",
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "cloudwatch:PutMetricData",
        "cloudwatch:GetMetricData",
        "dynamodb:GetItem",
        "dynamodb:Query",
        "dynamodb:Scan",
        "s3:*",
        "lambda:InvokeFunction"
      ],
      "Resource": [
        "*"
      ]
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "LambdaInlinePolicy" {
  name = "py_aws_iam_role_policy"
  role = "${aws_iam_role.iam_lambda_role.id}"
  policy = "${aws_iam_policy.LambdaInline.policy}"
}