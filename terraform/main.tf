resource "azurerm_resource_group" "rgroup" {
  name     = "nintendoproject"
  location = "Australia East"

}

resource "azurerm_storage_account" "stracc" {
  name                     = "nintendostorageaccount"
  location                 = azurerm_resource_group.rgroup.location
  resource_group_name      = azurerm_resource_group.rgroup.name
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true

}

resource "azurerm_role_assignment" "user_storage_contributor" {
  scope                = azurerm_storage_account.stracc.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = var.principal_id
}

resource "azurerm_storage_container" "containernintnedo" {
  name                  = "nintendo"
  storage_account_name  = azurerm_storage_account.stracc.name
  container_access_type = "private"
}

resource "azurerm_storage_data_lake_gen2_path" "inbound" {
  storage_account_id = azurerm_storage_account.stracc.id
  filesystem_name    = azurerm_storage_container.containernintnedo.name
  path               = "inbound"
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "bronze" {
  storage_account_id = azurerm_storage_account.stracc.id
  filesystem_name    = azurerm_storage_container.containernintnedo.name
  path               = "bronze"
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "silver" {
  storage_account_id = azurerm_storage_account.stracc.id
  filesystem_name    = azurerm_storage_container.containernintnedo.name
  path               = "silver"
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "gold" {
  storage_account_id = azurerm_storage_account.stracc.id
  filesystem_name    = azurerm_storage_container.containernintnedo.name
  path               = "gold"
  resource           = "directory"
}

resource "azurerm_service_plan" "srvplan" {
  name                = "nintendoservplan"
  location            = azurerm_resource_group.rgroup.location
  resource_group_name = azurerm_resource_group.rgroup.name
  os_type             = "Linux"
  sku_name            = "B1"

}

resource "azurerm_linux_function_app" "funcmagalu" {
  name                       = "appfuncmagalu"
  location                   = azurerm_resource_group.rgroup.location
  resource_group_name        = azurerm_resource_group.rgroup.name
  storage_account_name       = azurerm_storage_account.stracc.name
  storage_account_access_key = azurerm_storage_account.stracc.primary_access_key
  service_plan_id            = azurerm_service_plan.srvplan.id

  site_config {
    application_stack {
      python_version = "3.10"
    }
  }

  app_settings = {
    "FUNCTIONS_WORKER_RUNTIME" = "python"
    "AzureStorageConnection"   = azurerm_storage_account.stracc.primary_connection_string
    "OPENAI_API"               = var.openai_api_key
  }
}


resource "azurerm_linux_function_app" "funckabum" {
  name                       = "appfunckabum"
  location                   = azurerm_resource_group.rgroup.location
  resource_group_name        = azurerm_resource_group.rgroup.name
  storage_account_name       = azurerm_storage_account.stracc.name
  storage_account_access_key = azurerm_storage_account.stracc.primary_access_key
  service_plan_id            = azurerm_service_plan.srvplan.id

  site_config {
    application_stack {
      python_version = "3.10"
    }
  }

  app_settings = {
    "FUNCTIONS_WORKER_RUNTIME" = "python"
    "AzureStorageConnection"   = azurerm_storage_account.stracc.primary_connection_string
    "OPENAI_API"               = var.openai_api_key
  }
}


resource "azurerm_storage_management_policy" "inbound_delete_after_1_day" {
  storage_account_id = azurerm_storage_account.stracc.id

  rule {
    name    = "delete-inbound-after-1-day"
    enabled = true


    filters {
      prefix_match = ["nintendo/inbound"]
      blob_types   = ["blockBlob"]
    }


    actions {
      base_blob {

        delete_after_days_since_creation_greater_than = 1
      }
    }
  }
}

resource "azurerm_dashboard_grafana" "grafanamanager" {
  name                = "managedgrafanainstance"
  resource_group_name = azurerm_resource_group.rgroup.name
  location            = azurerm_resource_group.rgroup.location
  sku                 = "Standard"
  api_key_enabled     = true


  public_network_access_enabled = true

  identity {
    type = "SystemAssigned"
  }
}


output "grafana_endpoint" {
  value = azurerm_dashboard_grafana.grafanamanager.endpoint
}


resource "random_string" "naming" {
  special = false
  upper   = false
  length  = 6
}

locals {
  prefix = "nintendodatabricks${random_string.naming.result}"
}

resource "azurerm_databricks_workspace" "databricks_workspace" {
  name                        = "${local.prefix}-workspace"
  resource_group_name         = azurerm_resource_group.rgroup.name
  location                    = azurerm_resource_group.rgroup.location
  sku                         = "trial"
  managed_resource_group_name = "${local.prefix}-workspace-rg"

}


resource "azurerm_databricks_access_connector" "dac" {
  name                = "databricks-access-connector-demo"
  resource_group_name = azurerm_resource_group.rgroup.name
  location            = azurerm_resource_group.rgroup.location
  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "dac_storage_contributor" {
  scope                = azurerm_storage_account.stracc.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.dac.identity[0].principal_id
}

resource "databricks_user" "felipe_user" {
  user_name             = "felipegoraro@outlook.com"
  display_name          = "Felipe Pegoraro"
  workspace_access      = true
  databricks_sql_access = true
  allow_cluster_create  = true
}

resource "databricks_cluster" "dtb_cluster" {
  cluster_name            = "nintendo"
  spark_version           = "15.4.x-scala2.12"
  node_type_id            = "Standard_D4s_v3"
  driver_node_type_id     = "Standard_D4s_v3"
  autotermination_minutes = 10
  enable_elastic_disk     = true
  single_user_name        = databricks_user.felipe_user.user_name
  data_security_mode      = "SINGLE_USER"
  runtime_engine          = "PHOTON"

  autoscale {
    min_workers = 1
    max_workers = 2
  }

}

resource "databricks_git_credential" "github_connection" {
  git_username          = var.github_username
  git_provider          = "gitHub"
  personal_access_token = var.github_token
}