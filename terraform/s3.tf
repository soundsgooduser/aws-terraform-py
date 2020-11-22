resource "aws_s3_bucket" "s3_bucket_all_transactions" {
  bucket = "all-transactions"
}

resource "aws_s3_bucket_object" "object_transaction1" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "test1/transactions/transaction1.json"
  source = "../files/transaction1.json"
}

resource "aws_s3_bucket_object" "object_transaction2" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "test1/transactions/transaction2.json"
  source = "../files/transaction2.json"
}

resource "aws_s3_bucket_object" "object_transaction3" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "test2/transactions/transaction3.json"
  source = "../files/transaction3.json"
}

resource "aws_s3_bucket_object" "object_transaction4" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "test2/transactions/transaction4.json"
  source = "../files/transaction4.json"
}

resource "aws_s3_bucket_object" "object_transaction5" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "test3/transactions/transaction5.json"
  source = "../files/transaction5.json"
}

resource "aws_s3_bucket_object" "object_transaction6" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "test3/transactions/transaction6.json"
  source = "../files/transaction6.json"
}