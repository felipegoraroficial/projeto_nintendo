resource "azurerm_resource_group" "rgroup" {
  name     = "nintendoproject"
  location = "Australia East"

}

# Criação do App Registration no Azure AD
resource "azuread_application" "appreg" {
  display_name = "nintendo-aplication"
}

# Criação do Service Principal para o App Registration
resource "azuread_service_principal" "appreg_sp" {
  client_id = azuread_application.appreg.application_id
}

# Criação do Client Secret para o App Registration
resource "azuread_application_password" "appreg_secret" {
  application_object_id = azuread_application.appreg.object_id
  end_date_relative     = "8760h"
}

data "azurerm_client_config" "current" {}

output "client_id" {
  description = "O client_id da aplicação (Application ID)"
  value       = azuread_application.appreg.application_id
}

output "tenant_id" {
  description = "O tenant_id do Azure AD"
  value       = data.azurerm_client_config.current.tenant_id
}

output "client_secret" {
  description = "O valor do client secret gerado para o app registration"
  value       = azuread_application_password.appreg_secret.value
  sensitive   = true
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

# Criação do Key Vault
resource "azurerm_key_vault" "kv" {
  name                       = "nintendokeyvaultproject"
  location                   = azurerm_resource_group.rgroup.location
  resource_group_name        = azurerm_resource_group.rgroup.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7

  access_policy {
    tenant_id          = data.azurerm_client_config.current.tenant_id
    object_id          = data.azurerm_client_config.current.object_id
    secret_permissions = ["Get", "Set", "List", "Delete", "Recover", "Purge"]
  }
}

# Criação do Secret no Key Vault para armazenar a chave de acesso do Storage Account
resource "azurerm_key_vault_secret" "storage_key" {
  name         = "nintendo-storage-account-key-value"
  value        = azurerm_storage_account.stracc.primary_access_key
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_key_vault.kv, azurerm_storage_account.stracc]
}

# Armazenar o Client Secret do App Registration no Key Vault
resource "azurerm_key_vault_secret" "appreg_client_secret" {
  name         = "nintendo-aplication-client-secret-value"
  value        = azuread_application_password.appreg_secret.value
  key_vault_id = azurerm_key_vault.kv.id

  depends_on = [
    azurerm_key_vault.kv,
    azuread_application_password.appreg_secret
  ]
}

# Incluíndo permissão de acesso ao Key Vault para o App Registration
resource "azurerm_role_assignment" "appreg_blob_contributor" {
  scope                = azurerm_storage_account.stracc.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.appreg_sp.object_id
  depends_on = [
    azurerm_storage_account.stracc,
    azuread_service_principal.appreg_sp
  ]
}

resource "azurerm_role_assignment" "appreg_kv_access" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azuread_service_principal.appreg_sp.object_id
}

# Criação da política de gerenciamento do Storage Account para deletar blobs após 7 dia
resource "azurerm_storage_management_policy" "inbound_delete_after_1_day" {
  storage_account_id = azurerm_storage_account.stracc.id

  rule {
    name    = "delete-inbound-after-7-day"
    enabled = true


    filters {
      prefix_match = ["nintendo/inbound"]
      blob_types   = ["blockBlob"]
    }


    actions {
      base_blob {

        delete_after_days_since_creation_greater_than = 7
      }
    }
  }
}

# Criação do Data Factory
resource "azurerm_data_factory" "adf" {
  name                = "nintendoadf"
  location            = azurerm_resource_group.rgroup.location
  resource_group_name = azurerm_resource_group.rgroup.name

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_eventhub_namespace" "eventhub" {
  name                = "nintendoproject"
  location            = azurerm_resource_group.rgroup.location
  resource_group_name = azurerm_resource_group.rgroup.name
  sku                 = "Basic"
  capacity            = 1

}

resource "azurerm_eventhub" "hub" {
  name                = "nintendohub"
  namespace_name      = azurerm_eventhub_namespace.eventhub.name
  resource_group_name = azurerm_resource_group.rgroup.name
  partition_count     = 1
  message_retention   = 1
}

resource "azurerm_eventhub_authorization_rule" "rulehub" {
  name                = "nintendohubpolicy"
  namespace_name      = azurerm_eventhub_namespace.eventhub.name
  eventhub_name       = azurerm_eventhub.hub.name
  resource_group_name = azurerm_resource_group.rgroup.name
  listen              = false
  send                = true
  manage              = false
}

resource "azurerm_service_plan" "srvplan" {
  name                = "nintendoservplan"
  location            = azurerm_resource_group.rgroup.location
  resource_group_name = azurerm_resource_group.rgroup.name
  os_type             = "Linux"
  sku_name            = "B1"

}

resource "azurerm_application_insights" "app_insights" {
  name                = "nintendo-appinsights"
  location            = azurerm_resource_group.rgroup.location
  resource_group_name = azurerm_resource_group.rgroup.name
  application_type    = "other"
}

resource "azurerm_linux_function_app" "funcnintendo" {
  name                       = "appfuncnintendo"
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
    "FUNCTIONS_WORKER_RUNTIME"              = "python"
    "AzureStorageConnection"                = azurerm_storage_account.stracc.primary_connection_string
    "EventHubConnection"                    = azurerm_eventhub_authorization_rule.rulehub.primary_connection_string
    "OPENAI_API"                            = var.openai_api_key
    "APPINSIGHTS_INSTRUMENTATIONKEY"        = azurerm_application_insights.app_insights.instrumentation_key
    "APPLICATIONINSIGHTS_CONNECTION_STRING" = azurerm_application_insights.app_insights.connection_string
  }
}

resource "azurerm_databricks_workspace" "databricks_workspace" {
  name                        = "nintendodatabricks-workspace"
  resource_group_name         = azurerm_resource_group.rgroup.name
  location                    = azurerm_resource_group.rgroup.location
  sku                         = "trial"
  managed_resource_group_name = "nintendodatabricks-workspace-rg"

}


resource "azurerm_databricks_access_connector" "dac" {
  name                = "databricks-access-connector-demo"
  resource_group_name = azurerm_resource_group.rgroup.name
  location            = azurerm_resource_group.rgroup.location
  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "appreg_databricks_contributor" {
  scope                = azurerm_databricks_workspace.databricks_workspace.id
  role_definition_name = "Contributor"
  principal_id         = azuread_service_principal.appreg_sp.object_id

  depends_on = [
    azurerm_databricks_workspace.databricks_workspace
  ]
}


resource "azurerm_role_assignment" "dac_storage_contributor" {
  scope                = azurerm_storage_account.stracc.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.dac.identity[0].principal_id
}

resource "azurerm_role_assignment" "databricks_kv_access" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_databricks_access_connector.dac.identity[0].principal_id
}

# Criação do Secret Scope no Databricks, utilizando o Key Vault
resource "databricks_secret_scope" "keyvault_scope" {
  name                     = "nintendo-keyvault-scope"
  initial_manage_principal = "users"

  keyvault_metadata {
    resource_id = azurerm_key_vault.kv.id
    dns_name    = azurerm_key_vault.kv.vault_uri
  }

  depends_on = [
    azurerm_databricks_workspace.databricks_workspace,
    azurerm_role_assignment.databricks_kv_access
  ]
}

resource "databricks_service_principal" "app_sp" {
  application_id = azuread_application.appreg.application_id
  display_name   = "nintendo-databricks-sp"
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

  spark_conf = {
    "fs.azure.account.key.${azurerm_storage_account.stracc.name}.dfs.core.windows.net" = "{{secrets/${databricks_secret_scope.keyvault_scope.name}/${azurerm_key_vault_secret.storage_key.name}}}"
  }

}

resource "databricks_library" "pytest_lib" {
  cluster_id = databricks_cluster.dtb_cluster.id

  pypi {
    package = "pytest"
  }
}

resource "databricks_git_credential" "github_connection" {
  git_username          = var.github_username
  git_provider          = "gitHub"
  personal_access_token = var.github_token
}

resource "databricks_repo" "my_git_folder" {

  url = "https://github.com/felipegoraroficial/projeto_nintendo.git"

  path = "/Repos/${databricks_user.felipe_user.user_name}/projeto_nintendo"

  branch = "v2"

  depends_on = [
    azurerm_databricks_workspace.databricks_workspace,
    databricks_git_credential.github_connection
  ]
}