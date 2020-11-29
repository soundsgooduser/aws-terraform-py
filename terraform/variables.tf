variable "s3_keys_lambda_sender_name" {
  default = "s3_keys_lambda_sender"
}

variable "s3_keys_lambda_receiver_name" {
  default = "s3_keys_lambda_receiver"
}

variable "lambda_sender_handler" {
  default = "lambda_sender.lambda_handler"
}

variable "lambda_receiver_handler" {
  default = "lambda_receiver.lambda_handler"
}

variable "runtime" {
  default = "python3.6"
}