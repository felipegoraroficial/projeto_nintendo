# Databricks notebook source
# MAGIC %md
# MAGIC # Objetivo deste notebook
# MAGIC
# MAGIC  Este notebook tem como objetivo obter os registro de monitoramento da tablea lineage tables da system tables do databricks. 
# MAGIC
# MAGIC 1- faz leitura do arquivo json config para obter valores de atributos ao workflow em questão.
# MAGIC
# MAGIC 2 - obtém o caminho do notebook para identificar palavras referente ao ambiente e define a env em questão.
# MAGIC
# MAGIC 3- importa funções do repositório meus_scripts_pyspark.
# MAGIC
# MAGIC 4 - chama a função que faz a extração de dados e transformações da lineage tables para volumes e tables.
# MAGIC
# MAGIC 5 - padroniza os dataframes volumes e tables para unificação de ambos.
# MAGIC
# MAGIC 7 - salva os dados em uma tabela do catalog

# COMMAND ----------

import sys
import json
import os

# COMMAND ----------

# Obtém o caminho do diretório atual do notebook
current_path = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())

# COMMAND ----------

current_dir = '/'.join(current_path.split('/')[:4])

# Define o caminho do arquivo de configuração
config_path = f"/Workspace{current_dir}/config.json"

# Abre o arquivo de configuração e carrega seu conteúdo em um dicionário
with open(config_path, "r") as f:
    config = json.load(f)

# Obtém o valor das chaves do dicionário de configuração
token = config["token"]
current_instance = config["instance"]

#obter a instance_id a paritir do current_instance
instance_id = current_instance.split('-')[1].split('.')[0]

# COMMAND ----------

# Verifica se o caminho atual contém a string "dev"
if "dev" in current_path:
    # Define o ambiente como "dev"
    env = "dev"
# Verifica se o caminho atual contém a string "prd"
elif "prd" in current_path:
    # Define o ambiente como "prd"
    env = "prd"
# Caso contrário, define o ambiente como "env não encontrado"
else:
    env = "env não encontrado"

# COMMAND ----------

# Adiciona o caminho do diretório 'meus_scripts_pyspark' ao sys.path
# Isso permite que módulos Python localizados nesse diretório sejam importados
# Ajusta o caminho do diretório para os primeiros 3 níveis
current_dir = '/'.join(current_path.split('/')[:3])

sys.path.append(f'/Workspace{current_dir}/meus_scripts_pyspark')

# COMMAND ----------

from tables_lineage_tables_system import transform_lineage_tables
from volumes_lineage_tables_system import transform_lineage_volumes

# COMMAND ----------

#chamar função que extrai dados de jobs do databricks
volumes = transform_lineage_volumes(instance_id)

#filtrar dados do ambiente em questão
volumes = volumes.filter(volumes['containr'] == env)

#renomear coluna containr para identificar ambiente e diretorio para table para identificar a tabela
volumes = volumes.withColumnRenamed("containr", "env")
volumes = volumes.withColumnRenamed("diretorio", "table")

volumes = volumes.select('entity_run_id','entity_type','created_by','event_time','event_date','env','table','type','mode')

# COMMAND ----------

#chamar função que extrai dados de jobs do databricks
tables = transform_lineage_tables(instance_id)

#filtrar dados do ambiente em questão
tables = tables.filter(tables['schema'] == env)

#renomear coluna schema para identificar ambiente
tables = tables.withColumnRenamed("schema", "env")

#dropar coluna catalog
tables = tables.drop("catalog")

tables = tables.select('entity_run_id','entity_type','created_by','event_time','event_date','env','table','type','mode')

# COMMAND ----------

#unindo ambos dataframes para tornar uma unica tabela de monitoramento
df = tables.union(volumes)

# COMMAND ----------

#salva o dataframe em uma tabela do catalog do databricks
df.write.mode('overwrite').saveAsTable(f"{env}.`lineage-tables-monitoring`")
