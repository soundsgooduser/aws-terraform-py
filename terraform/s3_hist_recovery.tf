resource "aws_s3_bucket_object" "recovery_1" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-01-2020/1/us-east1/company/demo/1/11/response/device/111/11111111.txt"
  source = "../files/1.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_2" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-01-2020/1/us-east1/company/demo/2/2/response/device/222/222222222.txt"
  source = "../files/2.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_11111" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-01-2020/1/us-east1/company/demo/1/11/response/device/111/333333333.txt"
  source = "../files/1.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_222222" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-01-2020/1/us-east1/company/demo/2/2/response/device/222/4444444444.txt"
  source = "../files/2.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_3333333" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-01-2020/1/us-east1/company/demo/1/11/response/device/111/555555555.txt"
  source = "../files/1.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_4444444" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-01-2020/1/us-east1/company/demo/2/2/response/device/222/6666666666.txt"
  source = "../files/2.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_3" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-01-2020/2/us-east1/company/demo/3/3/response/device/333/3.txt"
  source = "../files/1.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_4" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-01-2020/2/us-east1/company/demo/4/4/response/device/444/4.txt"
  source = "../files/1.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_5" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-01-2020/3/us-east1/company/demo/5/5/response/device/555/5.txt"
  source = "../files/1.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_6" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-01-2020/3/us-east1/company/demo/6/6/response/device/666/6.txt"
  source = "../files/1.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_7" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-01-2020/4/us-east1/company/demo/7/7/response/device/777/7.txt"
  source = "../files/1.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_8" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-01-2020/4/us-east1/company/demo/8/8/response/device/888/8.txt"
  source = "../files/1.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_8_8" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-01-2020/5/us-east1/company/demo/88/88/response/device/8888/88.txt"
  source = "../files/1.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_9" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-03-2020/1/us-east1/company/demo/9/99/response/device/999/9.txt"
  source = "../files/1.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_10" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-03-2020/1/us-east1/company/demo/10/100/response/device/100/10.txt"
  source = "../files/1.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_11" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-03-2020/2/us-east1/company/demo/11/11/response/device/111/11.txt"
  source = "../files/1.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_12" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-03-2020/2/us-east1/company/demo/12/12/response/device/112/12.txt"
  source = "../files/1.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_13" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-03-2020/13/us-east1/company/demo/13/13/response/device/113/13.txt"
  source = "../files/1.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_14" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-03-2020/13/us-east1/company/demo/14/14/response/device/114/14.txt"
  source = "../files/1.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_15" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-10-2020/1/us-east1/company/demo/15/15/response/device/115/15.txt"
  source = "../files/1.txt"
  force_destroy = true
}

resource "aws_s3_bucket_object" "recovery_16" {
  bucket = "${aws_s3_bucket.s3_bucket_all_transactions.bucket}"
  key = "historical-recovery-path-with-dates/11-10-2020/1/us-east1/company/demo/16/16/response/device/116/16.txt"
  source = "../files/1.txt"
  force_destroy = true
}