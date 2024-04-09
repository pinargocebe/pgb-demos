variable "env" {
  description = "Environment name"
  type        = string
  nullable    = false
  default     = "default-env"
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
  default     = "default-account-id"
}

variable "s3_bucket_name" {
  description = "S3 bucket name"
  type        = string
  nullable    = false
  default     = "default-s3-bucket-name"
}