terraform {
  required_version = "= 1.16.1"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "= 6.57.1"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "beignet"
      Environment = "lab"
      ManagedBy   = "terraform"
    }
  }
}
