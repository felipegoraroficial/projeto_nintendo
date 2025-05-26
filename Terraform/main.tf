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
  is_hns_enabled           = true

}

resource "azurerm_service_plan" "srvplan" {
  name                = "nintendoservplan"
  location            = azurerm_resource_group.rgroup.location
  resource_group_name = azurerm_resource_group.rgroup.name
  os_type             = "Linux"
  sku_name            = "Y1"

}

resource "azurerm_linux_function_app" "funcapp" {
  name                       = "azfnintendo"
  location                   = azurerm_resource_group.rgroup.location
  resource_group_name        = azurerm_resource_group.rgroup.name
  storage_account_name       = azurerm_storage_account.stracc.name
  storage_account_access_key = azurerm_storage_account.stracc.primary_access_key
  service_plan_id            = azurerm_service_plan.srvplan.id
  site_config {

  }

}