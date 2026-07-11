
# ============================================================
# Provider AWS
# ============================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}


# ============================================================
# Variáveis
# ============================================================

variable "aws_region" {
  description = "Região AWS onde os recursos serão criados."
  type        = string
  default     = "sa-east-1"
}

variable "glue_job_name" {
  description = "Nome do AWS Glue Job."
  type        = string
  default     = "teste-eng-dados-etl-clientes"
}

variable "glue_role_arn" {
  description = "ARN da IAM Role utilizada pelo AWS Glue Job."
  type        = string
}

variable "script_s3_path" {
  description = "Caminho no S3 onde está armazenado o script PySpark."
  type        = string
  default     = "s3://bucket-scripts/teste_eng_dados/1.ETL/script.py"
}


# ============================================================
# AWS Glue Job
# ============================================================

resource "aws_glue_job" "etl_clientes" {
  name     = var.glue_job_name
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = var.script_s3_path
    python_version  = "3"
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 10

  max_retries = 1
  timeout     = 60

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"

    "--source-path"   = "s3://bucket-origem/clientes_sinteticos.csv"
    "--glue-database" = "db_clientes"
    "--bronze-table"  = "tabela_cliente_landing"
    "--silver-table"  = "tb_cliente"
  }

  tags = {
    projeto = "teste_eng_dados"
  }
}


# ============================================================
# Saídas
# ============================================================

output "glue_job_name" {
  description = "Nome do Glue Job criado."
  value       = aws_glue_job.etl_clientes.name
}

output "glue_job_arn" {
  description = "ARN do Glue Job criado."
  value       = aws_glue_job.etl_clientes.arn
}
