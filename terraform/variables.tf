variable "lambda_read_recovery_path_function_name" {
  default = "lambda-read-recovery-path"
}

variable "function_name" {
  default = "minimal_lambda_function"
}

variable "lambda_read_recovery_path" {
  default = "lambda_read_recovery_path.lambda_handler"
}

variable "handler" {
  default = "lambda.lambda_handler"
}

variable "runtime" {
  default = "python3.6"
}

