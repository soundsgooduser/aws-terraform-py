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

resource "aws_s3_bucket_object" "object_transaction11" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "us-east1/company/demo1/11112/2/response/device/2/Response.json"
  source = "../files/transaction1.json"
  force_destroy = true
}

resource "aws_s3_bucket_object" "object_transaction22" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "us-east1/company/demo1/11112/3/response/device/3/Response.json"
  source = "../files/transaction2.json"
  force_destroy = true
}

resource "aws_s3_bucket_object" "object_transaction2222" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "us-east1/company/demo1/11112/44/response/device/44/Response.json"
  source = "../files/transaction2.json"
  force_destroy = true
}

resource "aws_s3_bucket_object" "object_transaction2223" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "us-east1/company/demo1/11112/45/response/device/45/Response.json"
  source = "../files/transaction2.json"
  force_destroy = true
}

resource "aws_s3_bucket_object" "object_transaction2224" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "us-east1/company/demo1/11112/46/response/device/46/Response.json"
  source = "../files/transaction2.json"
  force_destroy = true
}

resource "aws_s3_bucket_object" "object_transaction2225" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "us-east1/company/demo1/11112/47/response/device/47/Response.json"
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

resource "aws_s3_bucket_object" "object_transaction666" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "us-east1/company/demo3/33333333333333333/111ef123/response/device/222d588/Response.bla"
  source = "../files/transaction6.json"
  force_destroy = true
}

resource "aws_s3_bucket_object" "object_transaction66666" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "us-east1/company/demo3/44444444444444444/111ef123/response/device/222d588/Response.txt"
  source = "../files/transaction6.json"
  force_destroy = true
}

resource "aws_s3_bucket_object" "object_transaction666666" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "us-east1/company/demo3/55555555555555555/111ef123/response/device/222d588/some-file.txt"
  source = "../files/transaction6.json"
  force_destroy = true
}

resource "aws_s3_bucket_object" "object_transaction66666666" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "us-east1/company/demo3/666666666666666666/111ef123/response/device/222d588/new-file.txt"
  source = "../files/transaction6.json"
  force_destroy = true
}

resource "aws_s3_bucket_object" "object_transaction66666666666" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "us-east1/company/demo3/777777777777777777/111ef123/response/device/222d588/new-file.csv"
  source = "../files/transaction6.json"
  force_destroy = true
}

#####

//resource "aws_s3_bucket_object" "object_transaction_0" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "historical-path/10-01-2020/us-east1/company/demo/0/0/response/device/1/0.txt"
//  source = "../files/0.txt"
//  force_destroy = true
//}
//
//resource "aws_s3_bucket_object" "object_transaction_1" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "historical-path/11-01-2020/us-east1/company/demo/1/1/response/device/1/1.txt"
//  source = "../files/1.txt"
//  force_destroy = true
//}
//
//resource "aws_s3_bucket_object" "object_transaction_2" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "historical-path/11-01-2020/us-east1/company/demo/2/2/response/device/2/2.txt"
//  source = "../files/2.txt"
//  force_destroy = true
//}
//
//resource "aws_s3_bucket_object" "object_transaction_3" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "historical-path/11-01-2020/us-east1/company/demo/3/3/response/device/3/3.txt"
//  source = "../files/3.txt"
//  force_destroy = true
//}
//
//resource "aws_s3_bucket_object" "object_transaction_4" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "historical-path/11-03-2020/us-east1/company/demo/4/4/response/device/4/4.txt"
//  source = "../files/4.txt"
//  force_destroy = true
//}
//
//resource "aws_s3_bucket_object" "object_transaction_5" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "historical-path/11-03-2020/us-east1/company/demo/5/5/response/device/5/5.txt"
//  source = "../files/5.txt"
//  force_destroy = true
//}
//
//resource "aws_s3_bucket_object" "object_transaction_6" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "historical-path/11-05-2020/us-east1/company/demo/6/6/response/device/6/6.txt"
//  source = "../files/6.txt"
//  force_destroy = true
//}
//
//resource "aws_s3_bucket_object" "object_transaction_7" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "historical-path/11-05-2020/us-east1/company/demo/7/7/response/device/7/7.txt"
//  source = "../files/7.txt"
//  force_destroy = true
//}
//
//resource "aws_s3_bucket_object" "object_transaction_8" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "historical-path/11-10-2020/us-east1/company/demo/8/8/response/device/8/8.txt"
//  source = "../files/8.txt"
//  force_destroy = true
//}
//
//resource "aws_s3_bucket_object" "object_transaction_9" {
//  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
//  key = "historical-path/11-09-2020/us-east1/company/demo/9/9/response/device/9/9.txt"
//  source = "../files/9.txt"
//  force_destroy = true
//}