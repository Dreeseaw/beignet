data "aws_caller_identity" "current" {}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_ssm_parameter" "al2023_arm64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

locals {
  name               = "beignet-lab"
  bucket_name        = "beignet-lab-${data.aws_caller_identity.current.account_id}-${var.region}"
  node_private_ips   = ["10.42.1.10", "10.42.1.11", "10.42.1.12"]
  control_binary     = abspath("${path.root}/${var.control_binary}")
  bench_binary       = abspath("${path.root}/${var.bench_binary}")
  control_object_key = "deploy/${filesha256(local.control_binary)}/beignet"
  bench_object_key   = "deploy/${filesha256(local.bench_binary)}/beignet-bench"
}

resource "aws_vpc" "lab" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = { Name = local.name }
}

resource "aws_subnet" "lab" {
  vpc_id                  = aws_vpc.lab.id
  cidr_block              = "10.42.1.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true

  tags = { Name = local.name }
}

resource "aws_internet_gateway" "lab" {
  vpc_id = aws_vpc.lab.id
  tags   = { Name = local.name }
}

resource "aws_route_table" "lab" {
  vpc_id = aws_vpc.lab.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.lab.id
  }

  tags = { Name = local.name }
}

resource "aws_route_table_association" "lab" {
  subnet_id      = aws_subnet.lab.id
  route_table_id = aws_route_table.lab.id
}

resource "aws_security_group" "nodes" {
  name        = local.name
  description = "Beignet Raft and HTTP traffic between lab nodes only"
  vpc_id      = aws_vpc.lab.id

  tags = { Name = local.name }
}

resource "aws_vpc_security_group_ingress_rule" "http" {
  security_group_id            = aws_security_group.nodes.id
  referenced_security_group_id = aws_security_group.nodes.id
  from_port                    = 4700
  to_port                      = 4700
  ip_protocol                  = "tcp"
  description                  = "Beignet HTTP between nodes"
}

resource "aws_vpc_security_group_ingress_rule" "raft" {
  security_group_id            = aws_security_group.nodes.id
  referenced_security_group_id = aws_security_group.nodes.id
  from_port                    = 7000
  to_port                      = 7000
  ip_protocol                  = "tcp"
  description                  = "Raft transport between nodes"
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.nodes.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "Package, S3, and Systems Manager access"
}

resource "aws_s3_bucket" "lab" {
  bucket        = local.bucket_name
  force_destroy = true

  tags = { Name = local.name }
}

resource "aws_s3_bucket_public_access_block" "lab" {
  bucket = aws_s3_bucket.lab.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lab" {
  bucket = aws_s3_bucket.lab.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lab" {
  bucket = aws_s3_bucket.lab.id

  rule {
    id     = "expire-disposable-lab-data"
    status = "Enabled"

    filter {}

    expiration {
      days = 1
    }
  }
}

resource "aws_s3_object" "control" {
  bucket      = aws_s3_bucket.lab.id
  key         = local.control_object_key
  source      = local.control_binary
  source_hash = filesha256(local.control_binary)
}

resource "aws_s3_object" "bench" {
  bucket      = aws_s3_bucket.lab.id
  key         = local.bench_object_key
  source      = local.bench_binary
  source_hash = filesha256(local.bench_binary)
}

data "aws_iam_policy_document" "node_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "node" {
  name               = "beignet-lab-node"
  assume_role_policy = data.aws_iam_policy_document.node_assume.json
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "node_s3" {
  statement {
    actions   = ["s3:GetBucketLocation", "s3:ListBucket"]
    resources = [aws_s3_bucket.lab.arn]
  }

  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lab.arn}/*"]
  }
}

resource "aws_iam_role_policy" "node_s3" {
  name   = "beignet-lab-s3"
  role   = aws_iam_role.node.id
  policy = data.aws_iam_policy_document.node_s3.json
}

resource "aws_iam_instance_profile" "node" {
  name = "beignet-lab-node"
  role = aws_iam_role.node.name
}

resource "aws_instance" "node" {
  count = 3

  ami                         = data.aws_ssm_parameter.al2023_arm64.value
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.lab.id
  private_ip                  = local.node_private_ips[count.index]
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.nodes.id]
  iam_instance_profile        = aws_iam_instance_profile.node.name

  instance_initiated_shutdown_behavior = "terminate"
  user_data_replace_on_change          = true
  user_data = templatefile("${path.module}/user-data.sh.tftpl", {
    node_number        = count.index + 1
    private_ip         = local.node_private_ips[count.index]
    leader_private_ip  = local.node_private_ips[0]
    bucket             = aws_s3_bucket.lab.id
    region             = var.region
    control_object_key = aws_s3_object.control.key
    bench_object_key   = aws_s3_object.bench.key
    benchmark_run_id   = var.benchmark_run_id
    worker_concurrency = var.worker_concurrency
    worker_batch_size  = var.worker_batch_size
    ttl_minutes        = var.ttl_minutes
  })

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 8
    encrypted             = true
    delete_on_termination = true
  }

  tags = {
    Name       = "${local.name}-node${count.index + 1}"
    Node       = "node${count.index + 1}"
    TTLMinutes = tostring(var.ttl_minutes)
  }

  depends_on = [
    aws_iam_role_policy_attachment.ssm,
    aws_iam_role_policy.node_s3,
    aws_route_table_association.lab,
  ]
}
