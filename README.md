# build and deploy to AWS

sh build.sh
cd terraform
terraform init
terraform plan
terraform apply
terraform destroy