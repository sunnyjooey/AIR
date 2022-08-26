provider "aws" {
  region = "us-east-1"
}

#resource "aws_instance" "example" {
#  ami = "ami-2d39803a"
#  instance_type = "t2.medium"
#}


#RDS cluster and instances for feature data storage
#resource "aws_rds_cluster" "default" {
#  cluster_identifier      = "aurora-cluster"
#  engine                  = "aurora"
#  database_name           = "mydb"
#  master_username         = "database_user"
#  master_password         = "made_up_string"
#  backup_retention_period = 5
#  preferred_backup_window = "07:00-09:00"
#  final_snapshot_identifier = "final-snapshot" #must have a snapshot name or can't delete to update
#}

#resource "aws_rds_cluster_instance" "cluster_instances" {
#  count              = 1
#  identifier         = "aurora-cluster-${count.index}"
#  cluster_identifier = "${aws_rds_cluster.default.id}"
#  instance_class     = "db.t2.micro"
#  publicly_accessible = "true"
#}


 #Elastic Container Repository for Docker images
#resource "aws_ecr_repository" "docker_container_repo" {
#  name = "teacher-effectiveness-app"
#}




#if it throws an error about which region it is being created in , it actually means the name is taken
#so change 'classroom-analytics' to something new (must be globally unique)

#resource "aws_s3_bucket" "classroom-analytics" {
#  bucket = "classroom-analytics"
#  acl    = "private"
#
#  tags {
#    Name        = "Classroom Analytics Video Bucket"
#  }
#}

