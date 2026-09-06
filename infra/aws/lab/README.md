# Disposable three-node AWS lab

This Terraform stack provisions exactly three ARM64 Beignet voters in one
`us-east-1` availability zone. Each `c7g.large` node also runs a separately
resource-limited synthetic worker process. The nodes have public IPs for
outbound access and Systems Manager, but the security group permits no public
ingress; HTTP and Raft traffic are allowed only between lab members.

The stack also creates one private, encrypted S3 bucket for binaries and
artifacts, a least-privilege instance role, an 8 GiB encrypted root volume per
node, and a guest shutdown timer. It does not create NAT gateways, load
balancers, Kubernetes clusters, or a container registry.

This is billable test infrastructure. Use a separate AWS Budget as a delayed
backstop, keep `ttl_minutes` bounded, and always destroy the stack after the run.
The guest timer terminates instances if the operator disappears, but it is not
a substitute for `terraform destroy` because non-instance resources remain.

## Build and plan

Terraform and provider versions are pinned. Authenticate the AWS CLI through a
short-lived profile before continuing; do not put credentials in `.tfvars`.

```bash
mkdir -p build/aws-arm64
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath \
  -o build/aws-arm64/beignet .
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath \
  -o build/aws-arm64/beignet-bench ./cmd/beignet-bench

terraform -chdir=infra/aws/lab init
terraform -chdir=infra/aws/lab plan \
  -var benchmark_run_id=aws-proof-unique \
  -var ttl_minutes=60 \
  -out=beignet.tfplan
```

Inspect the plan before applying it. A normal empty-account plan contains 22
creates: networking, S3, IAM, and three EC2 instances.

An EC2 `RunInstances --dry-run` result proves request authorization only. It
does not prove that AWS considers a new account fully verified for a real
launch. If apply fails at instance creation, run `terraform destroy`
immediately: networking, S3, and IAM resources may already exist even though no
instances were created.

## Apply, run, and clean up

```bash
terraform -chdir=infra/aws/lab apply beignet.tfplan
terraform -chdir=infra/aws/lab output benchmark_command
```

Run the printed benchmark command on node1 through AWS Systems Manager after
all three nodes are online and `/readyz` succeeds. Preserve the benchmark JSON,
worker summaries, start/end `/v1/status` responses, and service logs before
cleanup.

```bash
terraform -chdir=infra/aws/lab destroy \
  -var benchmark_run_id=aws-proof-unique \
  -var ttl_minutes=60
```

After destroy, confirm that the Terraform state has no resources and check AWS
for remaining `Project=beignet` EC2 instances, EBS volumes, VPCs, S3 buckets,
IAM roles, and instance profiles. A saved plan is stale after either an apply or
destroy; regenerate it for the next run.
