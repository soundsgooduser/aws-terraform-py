resource "aws_s3_bucket" "s3_bucket_all_transactions" {
  bucket = "all-transactions"
  force_destroy = true
}

resource "aws_s3_bucket_object" "object_transaction1" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "us-east1/company/demo1/1111/222ef123/response/device/333d588/Response.json"
  source = "../files/transaction1.json"
  force_destroy = true
}

resource "aws_s3_bucket_object" "object_transaction2" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "us-east1/company/demo1/1111/444ef123/response/device/555d588/Response.json"
  source = "../files/transaction2.json"
  force_destroy = true
}

resource "aws_s3_bucket_object" "object_transaction3" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "us-east1/company/demo2/2222/666ef123/response/device/777d588/Response.json"
  source = "../files/transaction3.json"
  force_destroy = true
}

resource "aws_s3_bucket_object" "object_transaction4" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "us-east1/company/demo2/2222/888ef123/response/device/999d588/Response.json"
  source = "../files/transaction4.json"
  force_destroy = true
}

resource "aws_s3_bucket_object" "object_transaction5" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "us-east1/company/demo3/3333/000ef123/response/device/111d588/Response.json"
  source = "../files/transaction5.json"
  force_destroy = true
}

resource "aws_s3_bucket_object" "object_transaction6" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "us-east1/company/demo3/3333/111ef123/response/device/222d588/Response.json"
  source = "../files/transaction6.json"
  force_destroy = true
}