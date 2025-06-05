resource "azurerm_resource_group" "rgroup" {
  name     = "nintendoproject"
  location = "Australia East"

}

resource "azurerm_storage_account" "stracc" {
  name                     = "nintendostorage"
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
  principal_id         = "8216f753-2fad-43f1-ac88-b7698c9786f0"
}

resource "azurerm_storage_container" "containernintnedo" {
  name                  = "nintendo"
  storage_account_name  = azurerm_storage_account.stracc.name
  container_access_type = "private"
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
  sku_name            = "Y1"

}

resource "azurerm_linux_function_app" "funcappcons" {
  name                       = "appnintendo"
  location                   = azurerm_resource_group.rgroup.location
  resource_group_name        = azurerm_resource_group.rgroup.name
  storage_account_name       = azurerm_storage_account.stracc.name
  storage_account_access_key = azurerm_storage_account.stracc.primary_access_key
  service_plan_id            = azurerm_service_plan.srvplan.id
  site_config {

  }

}

resource "azurerm_eventhub_namespace" "eventspace" {
  name                = "eventhubspacenintendo"
  location            = azurerm_resource_group.rgroup.location
  resource_group_name = azurerm_resource_group.rgroup.name
  sku                 = "Standard"
  capacity            = 2

}

resource "azurerm_eventhub" "nintendotopic" {
  name                = "nintendotopic"
  namespace_name      = azurerm_eventhub_namespace.eventspace.name
  resource_group_name = azurerm_resource_group.rgroup.name
  partition_count     = 2
  message_retention   = 2
}

resource "azurerm_eventhub_consumer_group" "nintendoconsumer" {
  name                = "nintendoconsumer"
  namespace_name      = azurerm_eventhub_namespace.eventspace.name
  eventhub_name       = azurerm_eventhub.nintendotopic.name
  resource_group_name = azurerm_resource_group.rgroup.name
}

resource "azurerm_databricks_workspace" "databricks_workspace" {
  name                        = "nintendodatabricks-workspace"
  resource_group_name         = azurerm_resource_group.rgroup.name
  location                    = azurerm_resource_group.rgroup.location
  sku                         = "trial"
  managed_resource_group_name = "nintendodatabricks-workspace-rg"
}

resource "databricks_cluster" "dtb_cluster" {
  cluster_name            = var.cluster_dev_name
  spark_version           = var.databricks_spark_version
  node_type_id            = var.databricks_node_type
  autotermination_minutes = 10
  data_security_mode      = "LEGACY_PASSTHROUGH"
  autoscale {
    min_workers = 1
    max_workers = 2
  }
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