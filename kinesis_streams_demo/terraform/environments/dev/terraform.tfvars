env = "dev"

region = "eu-west-1"

account_id = "000000000000"

kinesis_stream_name = "demo-event-stream"

iam_name_prefix = "demo-event-stream-app-role"

s3_bucket_name = "demo-event-stream-data"

processor_lambda_function_name = "demo-kinesis-processor-lambda"

glue_catalog_database_name = "demo-glue-catalog-db"

glue_catalog_table_name = "demo-glue-catalog-table"

glue_catalog_table_columns = {
  col1 = {
    "name" = "event_uuid",
    "type" = "string"
  }
  col2 = {
    "name" = "event_name",
    "type" = "string"
  }
  col3 = {
    "name" = "created_at",
    "type" = "timestamp"
  }
  col4 = {
    "name" = "created_datetime",
    "type" = "string"
  }
  col5 = {
    "name" = "event_type",
    "type" = "string"
  }
  col6 = {
    "name" = "event_subtype",
    "type" = "string"
  }
}