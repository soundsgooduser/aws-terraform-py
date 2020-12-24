resource "aws_s3_bucket" "s3_bucket_all_transactions" {
  bucket = "all-transactions"
  force_destroy = true
}

# 1
//resource "aws_s3_bucket_object" "object_transaction_1" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "us-east1/company/demo1/1111/222ef123/response/device/333d588/Response.json"
//  source = "../files/transaction1.json"
//  force_destroy = true
//}
//
////resource "aws_s3_bucket_object" "object_transaction_1_processed_success" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo1/1111/222ef123/response/device/333d588.e39b3cf977b7b5fa90760ee2b9eb7dcc.ods.processed.success"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
////resource "aws_s3_bucket_object" "object_transaction_1_processed_success_timestamp" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo1/1111/222ef123/response/device/333d588.e39b3cf977b7b5fa90760ee2b9eb7dcc.ods.processed.success.2020-01-01T00:00:00.100Z"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
//
//# 2
//resource "aws_s3_bucket_object" "object_transaction_2" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "us-east1/company/demo1/1111/444ef123/response/device/555d588/Response.json"
//  source = "../files/transaction2.json"
//  force_destroy = true
//}
//
////resource "aws_s3_bucket_object" "object_transaction_2_processed_success" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo1/1111/444ef123/response/device/555d588.1f6ba0009ade5ae06bf1b823ca82f5b9.ods.processed.success"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
////resource "aws_s3_bucket_object" "object_transaction_2_processed_success_timestamp" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo1/1111/444ef123/response/device/555d588.1f6ba0009ade5ae06bf1b823ca82f5b9.ods.processed.success.2020-02-02T00:00:00.200Z"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
//
//# 3
//resource "aws_s3_bucket_object" "object_transaction_11" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "us-east1/company/demo1/11112/2/response/device/2/Response.json"
//  source = "../files/transaction1.json"
//  force_destroy = true
//}
//
////resource "aws_s3_bucket_object" "object_transaction_11_processed_success" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo1/11112/2/response/device/2.e39b3cf977b7b5fa90760ee2b9eb7dcc.ods.processed.success"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
////resource "aws_s3_bucket_object" "object_transaction_11_processed_success_timestamp" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo1/11112/2/response/device/2.e39b3cf977b7b5fa90760ee2b9eb7dcc.ods.processed.success.2020-03-03T00:00:00.300Z"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
//
//# 4
//resource "aws_s3_bucket_object" "object_transaction_22" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "us-east1/company/demo1/11112/3/response/device/3/Response.json"
//  source = "../files/transaction2.json"
//  force_destroy = true
//}
//
////resource "aws_s3_bucket_object" "object_transaction_22_processed_success" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo1/11112/3/response/device/3.1f6ba0009ade5ae06bf1b823ca82f5b9.ods.processed.success"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
////resource "aws_s3_bucket_object" "object_transaction_22_processed_success_timestamp" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo1/11112/3/response/device/3.1f6ba0009ade5ae06bf1b823ca82f5b9.ods.processed.success.2020-04-04T00:00:00.400Z"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
//
//# 5
//resource "aws_s3_bucket_object" "object_transaction_2222" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "us-east1/company/demo1/11112/44/response/device/44/Response.json"
//  source = "../files/transaction2.json"
//  force_destroy = true
//}
//
////resource "aws_s3_bucket_object" "object_transaction_2222_processed_success" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo1/11112/44/response/device/44.1f6ba0009ade5ae06bf1b823ca82f5b9.ods.processed.success"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
////resource "aws_s3_bucket_object" "object_transaction_2222_processed_success_timestamp" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo1/11112/44/response/device/44.1f6ba0009ade5ae06bf1b823ca82f5b9.ods.processed.success.2020-05-05T00:00:00.500Z"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
//
//# 6
//resource "aws_s3_bucket_object" "object_transaction_2223" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "us-east1/company/demo1/11112/45/response/device/45/Response.json"
//  source = "../files/transaction2.json"
//  force_destroy = true
//}
//
////resource "aws_s3_bucket_object" "object_transaction_2223_processed_success" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo1/11112/45/response/device/45.1f6ba0009ade5ae06bf1b823ca82f5b9.ods.processed.success"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
////resource "aws_s3_bucket_object" "object_transaction_2223_processed_success_timestamp" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo1/11112/45/response/device/45.1f6ba0009ade5ae06bf1b823ca82f5b9.ods.processed.success.2020-06-06T00:00:00.600Z"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
//
//# 7
//resource "aws_s3_bucket_object" "object_transaction_2224" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "us-east1/company/demo1/11112/46/response/device/46/Response.json"
//  source = "../files/transaction2.json"
//  force_destroy = true
//}
//
////resource "aws_s3_bucket_object" "object_transaction_2224_processed_success" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo1/11112/46/response/device/46.1f6ba0009ade5ae06bf1b823ca82f5b9.ods.processed.success"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
////resource "aws_s3_bucket_object" "object_transaction_2224_processed_success_timestamp" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo1/11112/46/response/device/46.1f6ba0009ade5ae06bf1b823ca82f5b9.ods.processed.success.2020-07-07T00:00:00.700Z"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
//
//# 8
//resource "aws_s3_bucket_object" "object_transaction_2225" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "us-east1/company/demo1/11112/47/response/device/47/Response.json"
//  source = "../files/transaction2.json"
//  force_destroy = true
//}
//
////resource "aws_s3_bucket_object" "object_transaction_2225_processed_success" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo1/11112/47/response/device/47.1f6ba0009ade5ae06bf1b823ca82f5b9.ods.processed.success"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
////resource "aws_s3_bucket_object" "object_transaction_2225_processed_success_timestamp" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo1/11112/47/response/device/47.1f6ba0009ade5ae06bf1b823ca82f5b9.ods.processed.success.2020-08-08T00:00:00.800Z"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
//
//# 9
//resource "aws_s3_bucket_object" "object_transaction_3" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "us-east1/company/demo2/2222/666ef123/response/device/777d588/Response.json"
//  source = "../files/transaction3.json"
//  force_destroy = true
//}
//
////resource "aws_s3_bucket_object" "object_transaction_3_processed_success" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo2/2222/666ef123/response/device/777d588.7e2a0a4740667ecf0e5b27c147f72a9b.ods.processed.success"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
////resource "aws_s3_bucket_object" "object_transaction_3_processed_success_timestamp" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo2/2222/666ef123/response/device/777d588.7e2a0a4740667ecf0e5b27c147f72a9b.ods.processed.success.2020-09-09T00:00:00.900Z"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
//
//# 10
//resource "aws_s3_bucket_object" "object_transaction_4" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "us-east1/company/demo2/2222/888ef123/response/device/999d588/Response.json"
//  source = "../files/transaction4.json"
//  force_destroy = true
//}
//
////resource "aws_s3_bucket_object" "object_transaction_4_processed_success" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo2/2222/888ef123/response/device/999d588.de2731685728971b22e820d81ab41c5e.ods.processed.success"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
////resource "aws_s3_bucket_object" "object_transaction_4_processed_success_timestamp" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo2/2222/888ef123/response/device/999d588.de2731685728971b22e820d81ab41c5e.ods.processed.success.2020-10-10T00:00:00.101Z"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
//
//# 11
//resource "aws_s3_bucket_object" "object_transaction_5" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "us-east1/company/demo3/3333/000ef123/response/device/111d588/Response.json"
//  source = "../files/transaction5.json"
//  force_destroy = true
//}
//
////resource "aws_s3_bucket_object" "object_transaction_5_processed_success" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo3/3333/000ef123/response/device/111d588.33748e312ab27366e1fa7dbec035dcf1.ods.processed.success"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
////resource "aws_s3_bucket_object" "object_transaction_5_processed_success_timestamp" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo3/3333/000ef123/response/device/111d588.33748e312ab27366e1fa7dbec035dcf1.ods.processed.success.2020-11-11T00:00:00.111Z"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
//
//# 12
//resource "aws_s3_bucket_object" "object_transaction_6" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "us-east1/company/demo3/3333/111ef123/response/device/222d588/Response.json"
//  source = "../files/transaction6.json"
//  force_destroy = true
//}
//
////resource "aws_s3_bucket_object" "object_transaction_6_processed_success" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo3/3333/111ef123/response/device/222d588.36df7215eeed8448c02276c87692f4aa.ods.processed.success"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
////resource "aws_s3_bucket_object" "object_transaction_6_processed_success_timestamp" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo3/3333/111ef123/response/device/222d588.36df7215eeed8448c02276c87692f4aa.ods.processed.success.2020-01-01T00:00:00.100Z"
////  source = "../files/processed.success"
////  force_destroy = true
////}
//
//
//
//
//# Others not Response.json
////resource "aws_s3_bucket_object" "object_transaction666" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo3/33333333333333333/111ef123/response/device/222d588/Response.bla"
////  source = "../files/transaction6.json"
////  force_destroy = true
////}
////
////resource "aws_s3_bucket_object" "object_transaction66666" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo3/44444444444444444/111ef123/response/device/222d588/Response.txt"
////  source = "../files/transaction6.json"
////  force_destroy = true
////}
////
////resource "aws_s3_bucket_object" "object_transaction666666" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo3/55555555555555555/111ef123/response/device/222d588/some-file.txt"
////  source = "../files/transaction6.json"
////  force_destroy = true
////}
////
////resource "aws_s3_bucket_object" "object_transaction66666666" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo3/666666666666666666/111ef123/response/device/222d588/new-file.txt"
////  source = "../files/transaction6.json"
////  force_destroy = true
////}
////
////resource "aws_s3_bucket_object" "object_transaction66666666666" {
////  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
////  key = "us-east1/company/demo3/777777777777777777/111ef123/response/device/222d588/new-file.csv"
////  source = "../files/transaction6.json"
////  force_destroy = true
////}