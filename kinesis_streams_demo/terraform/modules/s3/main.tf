terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }

  required_version = ">= 1.7.5"
}


resource "aws_s3_bucket" "demo_s3_bucket" {
  bucket = "${var.env}-${var.s3_bucket_name}-${var.region}-${var.account_id}"
}


resource "aws_s3_bucket_acl" "demo_s3_bucket_acl" {
  bucket = aws_s3_bucket.demo_s3_bucket.id
  acl    = "private"
}