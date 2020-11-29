variable "function_name" {
  default = "minimal_lambda_function"
}

variable "s3_keys_lambda_receiver_name" {
  default = "s3_keys_lambda_receiver"
}

variable "handler" {
  default = "lambda.lambda_handler"
}

variable "lambda_receiver_handler" {
  default = "lambda_receiver.lambda_handler"
}

variable "runtime" {
  default = "python3.6"
}