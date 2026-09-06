output "bucket" {
  value = aws_s3_bucket.lab.id
}

output "instance_ids" {
  value = aws_instance.node[*].id
}

output "node_private_ips" {
  value = local.node_private_ips
}

output "benchmark_run_id" {
  value = var.benchmark_run_id
}

output "benchmark_command" {
  value = "/usr/local/bin/beignet-bench run --targets http://${join(":4700,http://", local.node_private_ips)}:4700 --run ${var.benchmark_run_id} --turns 100000 --submit-concurrency 128 --submit-batch-size 128 --workers 0 --audit-interval 25ms --timeout 10m"
}
