# ---------------------------------------------------------
# 1. Glue ke liye IAM Role
# ---------------------------------------------------------
resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# Glue managed policy
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Script bucket + logs ke liye custom policy
resource "aws_iam_role_policy" "glue_s3_logs" {
  name = "${var.project_name}-glue-s3-logs"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.script_bucket}",
          "arn:aws:s3:::${var.script_bucket}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# ---------------------------------------------------------
# 2. Script ko S3 par upload karo (etag se change detect hoga)
# ---------------------------------------------------------
resource "aws_s3_object" "glue_script" {
  bucket = var.script_bucket
  key    = var.glue_script_key
  source = "${path.module}/glue_script.py"

  # YE LINE SABSE ZAROORI HAI — script change hote hi Terraform update karega
  etag = filemd5("${path.module}/glue_script.py")

  tags = {
    Name = "${var.project_name}-glue-script"
  }
}

# ---------------------------------------------------------
# 3. Glue Job
# ---------------------------------------------------------
resource "aws_glue_job" "this" {
  name     = "${var.project_name}-job"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_object.glue_script.bucket}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }

  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  glue_version      = "4.0"

  execution_property {
    max_concurrent_runs = 5
  }

  # Optional: job bookmarks
  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }

  tags = {
    Name = "${var.project_name}-glue-job"
  }
}