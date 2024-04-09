terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4.2"
    }
  }

  required_version = ">= 1.7.5"
}


/*
* Define kinesis data stream and kinesis firehose
*/

resource "aws_kinesis_stream" "demo_stream" {
  name             = "${var.env}-${var.kinesis_stream_name}"
  shard_count      = 4
  retention_period = 24

  shard_level_metrics = [
    "IncomingBytes",
    "OutgoingBytes",
  ]


  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    Environment = var.env
  }
}


resource "aws_kinesis_firehose_delivery_stream" "demo_delivery_stream" {
  name        = "${var.env}-${var.kinesis_stream_name}-delivery"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = var.s3_bucket_arn

    buffer_size     = 5
    buffer_interval = 300

    dynamic_partitioning_configuration {
      enabled = "true"
    }

    prefix              = "event-streams/data/"
    error_output_prefix = "event-streams/errors/"


    processing_configuration {
      enabled = "true"

      processors {
        type = "Lambda"

        parameters {
          parameter_name  = "LambdaArn"
          parameter_value = "${aws_lambda_function.lambda_processor.arn}:$LATEST"
        }
      }
    }

    data_format_conversion_configuration {
      input_format_configuration {
        deserializer {
          hive_json_ser_de {}
        }
      }

      output_format_configuration {
        serializer {
          parquet_ser_de {}
        }
      }

      schema_configuration {
        database_name = aws_glue_catalog_table.glue_catalog_table.database_name
        table_name    = aws_glue_catalog_table.glue_catalog_table.name
        role_arn      = aws_iam_role.firehose.arn
        region        = var.region
      }
    }
  }

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.demo_stream.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  tags = {
    Environment = var.env
  }
}


/*
* Firehose roles 
*/

resource "aws_iam_role" "firehose" {
  name = "DemoFirehoseAssumeRole"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "firehose.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}


resource "aws_iam_policy" "firehose_s3" {
  name_prefix = "${var.env}-${var.iam_name_prefix}-"
  policy      = <<-EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
        "Sid": "",
        "Effect": "Allow",
        "Action": [
            "s3:AbortMultipartUpload",
            "s3:GetBucketLocation",
            "s3:GetObject",
            "s3:ListBucket",
            "s3:ListBucketMultipartUploads",
            "s3:PutObject"
        ],
        "Resource": [
            "${var.s3_bucket_arn}",
            "${var.s3_bucket_arn}/*"
        ]
    }
  ]
}
EOF
}


resource "aws_iam_policy" "firehose_glue" {
  name_prefix = "${var.env}-${var.iam_name_prefix}-"
  policy      = <<-EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
        "Sid": "",
        "Effect": "Allow",
        "Action": [
            "glue:GetTableVersions"
        ],
        "Resource": [
            "*"
        ]
    }
  ]
}
EOF
}


resource "aws_iam_role_policy_attachment" "firehose_glue" {
  role       = aws_iam_role.firehose.name
  policy_arn = aws_iam_policy.firehose_glue.arn
}


resource "aws_iam_role_policy_attachment" "firehose_s3" {
  role       = aws_iam_role.firehose.name
  policy_arn = aws_iam_policy.firehose_s3.arn
}


resource "aws_iam_policy" "put_record" {
  name_prefix = "${var.env}-${var.iam_name_prefix}-"
  policy      = <<-EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "firehose:PutRecord",
                "firehose:PutRecordBatch"
            ],
            "Resource": [
                "${aws_kinesis_firehose_delivery_stream.demo_delivery_stream.arn}"
            ]
        }
    ]
}
EOF
}


resource "aws_iam_role_policy_attachment" "put_record" {
  role       = aws_iam_role.firehose.name
  policy_arn = aws_iam_policy.put_record.arn
}


resource "aws_iam_policy" "kinesis_firehose" {
  name_prefix = "${var.env}-${var.iam_name_prefix}-"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
        "Sid": "",
        "Effect": "Allow",
        "Action": [
            "kinesis:DescribeStream",
            "kinesis:GetShardIterator",
            "kinesis:GetRecords",
            "kinesis:ListShards"
        ],
        "Resource": "${aws_kinesis_stream.demo_stream.arn}"
    }
  ]
}
EOF
}


resource "aws_iam_role_policy_attachment" "kinesis_firehose" {
  role       = aws_iam_role.firehose.name
  policy_arn = aws_iam_policy.kinesis_firehose.arn
}


/*
* Define Processing Lambda  
*/

data "archive_file" "zip" {
  type        = "zip"
  source_file = "../../../app/consumer/lambda_function.py"
  output_path = "lambda_processor.zip"
}

resource "aws_lambda_function" "lambda_processor" {
  filename         = data.archive_file.zip.output_path
  source_code_hash = filebase64sha256(data.archive_file.zip.output_path)

  function_name = "${var.env}-${var.processor_lambda_function_name}"
  role          = aws_iam_role.lambda_iam_role.arn
  runtime       = "python3.8"
  handler       = "lambda_function.lambda_handler"
  timeout       = 60

}


/*
* Lambda roles 
*/

resource "aws_iam_role" "lambda_iam_role" {
  name = "lambda_iam_to_s3"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

}

resource "aws_iam_policy" "lambda_policy" {
  name = "api-gateway-to-sqs-role-policy_new"

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams",
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:FilterLogEvents"
        ],
        "Resource": "*"
      },
      {
        "Effect": "Allow",
        "Action": [
          "s3:AbortMultipartUpload",
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:PutObject"
        ],
        "Resource": "${var.s3_bucket_arn}"
      }
    ]
}
EOF
}


resource "aws_iam_role_policy_attachment" "policy_to_role" {
  role       = aws_iam_role.lambda_iam_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}


/*
* Define Glue catalog
*/

resource "aws_glue_catalog_database" "glue_catalog_database" {
  name       = "${var.env}-${var.glue_catalog_database_name}"
  catalog_id = var.account_id
}

resource "aws_glue_catalog_table" "glue_catalog_table" {
  name          = "${var.env}-${var.glue_catalog_table_name}"
  database_name = aws_glue_catalog_database.glue_catalog_database.name
  catalog_id    = var.account_id

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  target_table {
    catalog_id    = var.account_id
    database_name = aws_glue_catalog_database.glue_catalog_database.name
    name          = "${var.env}-${var.glue_catalog_table_name}"
  }


  storage_descriptor {
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    location      = "s3://${var.env}-${var.s3_bucket_name}-${var.region}-${var.account_id}/event-streams"

    ser_de_info {
      name                  = "JsonSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = 1
      }
    }

    dynamic "columns" {
      for_each = var.glue_catalog_table_columns
      content {
        name = columns.value["name"]
        type = columns.value["type"]
      }
    }
  }
}