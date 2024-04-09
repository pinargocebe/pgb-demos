output "s3_bucket_arn" {
  value       = aws_s3_bucket.demo_s3_bucket.arn
  description = "ARN of the S3 bucket"
}