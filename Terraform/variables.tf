variable "databricks_node_type" {
  type    = string
  default = "Standard_D3_v2"
}

variable "databricks_spark_version" {
  type    = string
  default = "10.4.x-scala2.12"
}

variable "cluster_dev_name" {
  type    = string
  default = "nintendo"
}
