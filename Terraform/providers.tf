terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>3.51.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 1.0"
    }
    random = "~> 2.2"
    databricks = {
      source = "databricks/databricks"
    }
  }
}


provider "azurerm" {
  skip_provider_registration = true
  features {}
}

provider "databricks" {
  azure_workspace_resource_id = azurerm_databricks_workspace.databricks_workspace.id
  azure_use_msi               = true
}