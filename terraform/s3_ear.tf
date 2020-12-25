resource "aws_s3_bucket_object" "object_transaction_122" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-22/cust_1/ref_1/tr_1.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_222" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-22/cust_2/ref_2/tr_2.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_322" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-22/cust_3/ref_3/tr_3.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_422" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-22/cust_4/ref_4/tr_4.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_522" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-22/cust_5/ref_5/tr_5.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_622" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-22/cust_6/ref_6/tr_6.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_722" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-22/cust_7/ref_7/tr_7.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_822" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-22/cust_8/ref_8/tr_8.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_922" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-22/cust_9/ref_9/tr_9"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_1022" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-22/cust_10/ref_10/tr_10"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_1122" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-22/cust_11/ref_11/tr_11"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_1222" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-22/cust_12/ref_12/tr_12"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_1322" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-22/cust_13/ref_13/tr_13"
  source = "../files/transaction1.json"
  force_destroy = true
}

####

//resource "aws_s3_bucket_object" "object_transaction_123" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "rules/2020-11-23/cust_1/ref_1/tr_1.json"
//  source = "../files/transaction1.json"
//  force_destroy = true
//}
//resource "aws_s3_bucket_object" "object_transaction_223" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "rules/2020-11-23/cust_2/ref_2/tr_2.json"
//  source = "../files/transaction1.json"
//  force_destroy = true
//}
//resource "aws_s3_bucket_object" "object_transaction_323" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "rules/2020-11-23/cust_3/ref_3/tr_3.json"
//  source = "../files/transaction1.json"
//  force_destroy = true
//}
//resource "aws_s3_bucket_object" "object_transaction_423" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "rules/2020-11-23/cust_4/ref_4/tr_4.json"
//  source = "../files/transaction1.json"
//  force_destroy = true
//}
//resource "aws_s3_bucket_object" "object_transaction_523" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "rules/2020-11-23/cust_5/ref_5/tr_5.json"
//  source = "../files/transaction1.json"
//  force_destroy = true
//}
//resource "aws_s3_bucket_object" "object_transaction_623" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "rules/2020-11-23/cust_6/ref_6/tr_6.json"
//  source = "../files/transaction1.json"
//  force_destroy = true
//}
//resource "aws_s3_bucket_object" "object_transaction_723" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "rules/2020-11-23/cust_7/ref_7/tr_7.json"
//  source = "../files/transaction1.json"
//  force_destroy = true
//}
//resource "aws_s3_bucket_object" "object_transaction_823" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "rules/2020-11-23/cust_8/ref_8/tr_8.json"
//  source = "../files/transaction1.json"
//  force_destroy = true
//}
//resource "aws_s3_bucket_object" "object_transaction_923" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "rules/2020-11-23/cust_9/ref_9/tr_9.json"
//  source = "../files/transaction1.json"
//  force_destroy = true
//}
//resource "aws_s3_bucket_object" "object_transaction_1023" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "rules/2020-11-23/cust_10/ref_10/tr_10.json"
//  source = "../files/transaction1.json"
//  force_destroy = true
//}
//resource "aws_s3_bucket_object" "object_transaction_1123" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "rules/2020-11-23/cust_11/ref_11/tr_11.json"
//  source = "../files/transaction1.json"
//  force_destroy = true
//}
//resource "aws_s3_bucket_object" "object_transaction_1223" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "rules/2020-11-23/cust_12/ref_12/tr_12.json"
//  source = "../files/transaction1.json"
//  force_destroy = true
//}
//resource "aws_s3_bucket_object" "object_transaction_1323" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "rules/2020-11-23/cust_13/ref_13/tr_13.json"
//  source = "../files/transaction1.json"
//  force_destroy = true
//}

##################

resource "aws_s3_bucket_object" "object_transaction_124" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-24/cust_1/ref_1/tr_1.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_224" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-24/cust_2/ref_2/tr_2.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_324" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-24/cust_3/ref_3/tr_3.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_424" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-24/cust_4/ref_4/tr_4.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_524" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-24/cust_5/ref_5/tr_5.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_624" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-24/cust_6/ref_6/tr_6.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_724" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-24/cust_7/ref_7/tr_7.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_824" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-24/cust_8/ref_8/tr_8.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_924" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-24/cust_9/ref_9/tr_9.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_1024" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-24/cust_10/ref_10/tr_10.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_1124" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-24/cust_11/ref_11/tr_11.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_1224" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-24/cust_12/ref_12/tr_12.json"
  source = "../files/transaction1.json"
  force_destroy = true
}
resource "aws_s3_bucket_object" "object_transaction_1324" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "rules/2020-11-24/cust_13/ref_13/tr_13.json"
  source = "../files/transaction1.json"
  force_destroy = true
}