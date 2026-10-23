output "glue_job_name" {
  description = "Glue job ka naam"
  value       = aws_glue_job.this.name
}

output "glue_job_arn" {
  description = "Glue job ka ARN"
  value       = aws_glue_job.this.arn
}

output "glue_role_arn" {
  description = "Glue IAM role ka ARN"
  value       = aws_iam_role.glue_role.arn
}

output "glue_script_s3_path" {
  description = "Script ka S3 path"
  value       = "s3://${aws_s3_object.glue_script.bucket}/${aws_s3_object.glue_script.key}"
}