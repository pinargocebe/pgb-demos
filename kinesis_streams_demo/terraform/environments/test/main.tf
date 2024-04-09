terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }

  required_version = ">= 1.7.5"
}


provider "aws" {
  region = var.region
}


module "kinesis" {
  source = "../../modules/kinesis"

  env = var.env

  region = var.region

  account_id = var.account_id

  kinesis_stream_name = var.kinesis_stream_name

  iam_name_prefix = var.iam_name_prefix

  s3_bucket_name = var.s3_bucket_name

  s3_bucket_arn = module.s3.s3_bucket_arn

  processor_lambda_function_name = var.processor_lambda_function_name

  glue_catalog_database_name = var.glue_catalog_database_name

  glue_catalog_table_name = var.glue_catalog_table_name

  glue_catalog_table_columns = var.glue_catalog_table_columns
}


module "s3" {
  source = "../../modules/s3"

  env = var.env

  region = var.region

  account_id = var.account_id

  s3_bucket_name = var.s3_bucket_name
}