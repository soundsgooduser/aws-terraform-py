variable "function_name" {
  default = "minimal_lambda_function"
}

variable "handler" {
  default = "lambda.lambda_handler"
}

variable "runtime" {
  default = "python3.6"
}