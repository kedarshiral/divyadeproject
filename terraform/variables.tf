variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name prefix"
  type        = string
  default     = "divyadeproject"
}

variable "script_bucket" {
  description = "S3 bucket jahan CodePipeline script daalta hai"
  type        = string
  default     = "project-glue-asset-bundle "  # apna actual bucket naam daalo
}

variable "glue_script_key" {
  description = "S3 key (path) jahan script hai"
  type        = string
  default     = "scripts/glue_script.py"
}

variable "glue_worker_type" {
  description = "Glue worker type"
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Glue workers count"
  type        = number
  default     = 2
}