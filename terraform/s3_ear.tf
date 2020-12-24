resource "aws_s3_bucket_object" "object_transaction_1" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-22/cust/ref/1111.json"
  source = "../files/transaction1.json"
  force_destroy = true
}