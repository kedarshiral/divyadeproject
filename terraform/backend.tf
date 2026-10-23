terraform {
  backend "s3" {
    bucket       = "gluetestbucketinsss"
    key          = "glue/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true   # Terraform 1.10+ ke liye; purana version ho toh dynamodb_table use karo
  }
}