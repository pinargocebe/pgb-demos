variable "env" {
  description = "Environment name"
  type        = string
  nullable    = false
  default     = "dev"
}


variable "region" {
  description = "Aws region"
  type        = string
  nullable    = false
  default     = "eu-west-1"
}


variable "account_id" {
  description = "Aws account id"
  type        = string
  nullable    = false
  default     = "12323544567657"
}


variable "kinesis_stream_name" {
  description = "Kinesis Data Stream Name"
  type        = string
  nullable    = false
  default     = "default-kinesis-stream"
}


variable "iam_name_prefix" {
  description = "Prefix used for all created IAM roles and policies"
  type        = string
  nullable    = false
  default     = "default-iam-role-prefix"
}


variable "s3_bucket_name" {
  description = "S3 bucket name"
  type        = string
  nullable    = false
  default     = "default-s3-bucket-name"
}

variable "processor_lambda_function_name" {
  description = "Lambda function name for kinesis firehose processor lambda"
  type        = string
  nullable    = false
  default     = "default-kinesis-processor-lambda"
}

variable "glue_catalog_database_name" {
  description = "The Glue catalog database name"
  type        = string
}

variable "glue_catalog_table_name" {
  description = "The Glue catalog database table name"
  type        = string
}

variable "glue_catalog_table_columns" {
  description = "A list of table columns"
  type = map(object({
    name = string
    type = string
  }))
}