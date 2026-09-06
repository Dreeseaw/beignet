variable "region" {
  description = "AWS region for the disposable lab."
  type        = string
  default     = "us-east-1"

  validation {
    condition     = var.region == "us-east-1"
    error_message = "This bounded lab is restricted to us-east-1."
  }
}

variable "instance_type" {
  description = "ARM64 instance type for each of the three converged nodes."
  type        = string
  default     = "c7g.large"

  validation {
    condition     = contains(["t4g.small", "c7g.large"], var.instance_type)
    error_message = "Only t4g.small or c7g.large are allowed by this lab."
  }
}

variable "ttl_minutes" {
  description = "Instance lifetime before the guest shuts down and EC2 terminates it."
  type        = number
  default     = 120

  validation {
    condition     = var.ttl_minutes >= 15 && var.ttl_minutes <= 120
    error_message = "ttl_minutes must be between 15 and 120."
  }
}

variable "benchmark_run_id" {
  description = "Run identifier shared by submissions and synthetic workers."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9._-]{1,64}$", var.benchmark_run_id))
    error_message = "benchmark_run_id must be 1-64 letters, digits, dots, underscores, or hyphens."
  }
}

variable "control_binary" {
  description = "Path to the Linux ARM64 Beignet control-plane binary."
  type        = string
  default     = "../../../build/aws-arm64/beignet"
}

variable "bench_binary" {
  description = "Path to the Linux ARM64 synthetic benchmark binary."
  type        = string
  default     = "../../../build/aws-arm64/beignet-bench"
}

variable "worker_concurrency" {
  description = "Synthetic executor slots represented on each converged node."
  type        = number
  default     = 512

  validation {
    condition     = var.worker_concurrency >= 1 && var.worker_concurrency <= 1024
    error_message = "worker_concurrency must be between 1 and 1024."
  }
}

variable "worker_batch_size" {
  description = "Synthetic executor slots in each claim and commit request."
  type        = number
  default     = 64

  validation {
    condition     = var.worker_batch_size >= 1 && var.worker_batch_size <= 256
    error_message = "worker_batch_size must be between 1 and 256."
  }
}
