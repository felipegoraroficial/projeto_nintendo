# WebScrapy de Anúncios de Dados de Consoles Nintendo Switch V2

## Objetivo:

O objetivo deste projeto é a captura de dados em quase tempo real referentes a anúncios de vendas em e-commerce/marketplaces de forma escalável, com foco específico nos consoles Nintendo Switch. Os dados capturados são tratados e armazenados em um ambiente na nuvem, permitindo sua integração com ferramentas de visualização de dados (DataViz). Isso possibilita a geração de gráficos e insights para identificar os melhores preços/oportunidades e as principais características dos consoles em diferentes marketplaces.

<div align="center">
  <img src="https://github.com/user-attachments/assets/44df77ae-63ca-4d69-8c58-7820cdb98b5a" alt="Desenho Técnico">
  <p><b>Desenho Funcional</b></p>
</div>

<p align="left">
</p>

## Introdução:

O projeto foi desenvolvido utilizando as linguagens de programação Python para a aplicação no AZ Function e PySpark e SparkSQL na paltaforma de nuvem da Azure Databricks para a modelagem de dados na arquitetura medalhão e criação de tabelas fato e dimenssão bem como metricas e indicadores para posteriores analises de BI.

Utilizando o Azure Functions como produtor para realizar requisições em quase tempo real, aplicando técnicas de scraping com o apoio de um agente de IA para extrair os conteúdos HTML necessários, que em seguida são enviados ao Azure Event Hub.

Após o recebimento das mensagens no Event Hub, o recurso Azure Stream Analytics foi utilizado como consumidor para processá-las e enviá-las de forma incremental ao diretório inbound da Storage Account.

Com Azure Databricks como plataforma de pipeline de dados na arquitetura medalhão, foi utilizando a linguegam Pyspark para lidar com grandes volume de dados na stage bronze, silver e gold e SparkSQL para criação de analise de dados e BI.

Para a criação do Job no databricks, foi utilizando Delta Live Tables (DLT) que facilita a construção do pipeline de dados além de dar suporte a qualidade de dados e linhagem de dados.

Para o monitoramento desses jobs na paltaforma do Databricks e todos os recursos da Azure, foi criado painéis no Grafna utilizando Cluster Serveless Warehouse que  conecta a tabelas da System Tables no Databricks e Azure Monitor com Aplications Insights para monitoramento das functions no AZ Functions, Event Hub e StorageAccount.

<div align="center">
  <img src="https://github.com/user-attachments/assets/4d5a7b36-5d15-4751-9665-8849e4fffbd6" alt="Desenho Técnico">
  <p><b>Desenho Técnico</b></p>
</div>

<p align="left">
</p>


## Meta:

1. **Captação de Dados Brutos em Quase Tempo Real**:
    - **Objetivo**: Criar uma aplicação no Azure Functions para capturar dados brutos diários de sites de e-commerce e marketplaces para análise posterior.
    - **Benefício**: Permite produzir dados em quase tempo real com menor custo.
  
2. **Ingestão de dados em Streaming**:
    - **Objetivo**: Criar consumidor streaming de forma incremental no storageaccount.
    - **Benefício**: Permite a captura de dados em streaming utilizando recurso interátivo na Azure de baixa compelxidade e de menor custo.

3. **Escalando Captura de Elemento HTML**:
    - **Objetivo**: Utilizar modelos de IA para extrair informações essenciais dos arquivos HTML, como links, títulos, preços, promoções, parcelamentos e imagens dos produtos.
    - **Benefício**: Escalabilidade para ampliar o leque de fonte de dados e flexibilidade para processo produtivo com menor probabilidade para correções de bugs.

3. **Processamento e Escalabilidade para BigData**:
    - **Objetivo**: Aplicar processamento Spark ao conjuntos de dados na arquitetura medalhão do pipeline.
    - **Benefício**: Melhoria da qualidade dos dados, com correção de dados e padronização do mesmo e, além disso, o processamento de dados escalonáveis e suportando big data.

4. **Data Lakehouse Estrutura Medalhão de Dados**:
    - **Objetivo**: Transformar o datalake (storageaccount) em um data lakehouse.
    - **Benefício**: Combinando a flexibilidade e escalabilidade de um data lake com a confiabilidade e performance de um data warehouse.

5. **Pipeline ELT com DLT**:
    - **Objetivo**: Criar um pipeline de dados com baixa complexidade de desenvolvimento e com grandes ganho em qualidade e monitoramento de dados.
    - **Benefício**: o DLT transforma pipelines de ETL/ELT em algo mais confiável, menos manual e pronto para escalar, acelerando a entrega de valor com dados.

6. **Otimizando o processo para análise e BI**:
    - **Objetivo**: Utilização do Spark SQL no Databricks para a criação de tabelas fato e dimensão a partir de uma tabela Gold do Data Lakehouse.
    - **Benefício**: Permite uma modelagem de dados analíticos eficiente e confiável, combinando a familiaridade do SQL com a escalabilidade do Spark e a integridade do Delta Lake.

7. **Monitoramento de Processos**:
    - **Objetivo**: Monitoramento de todos recurtsos e status do projeto.
    - **Benefício**: Monitoramento de todas as funcionalidades e recursos da arquitetura do projeto em um único local (Grafana).

## Construção do Ambiente com Terraform:

A estrutura do Terraform é referente ao modelo abaixo:

```plaintext
terraform/
│
├── .terraform/                 # Pasta interna do Terraform (plugins, cache, metadados)
├── .gitignore                   # Arquivo para ignorar arquivos/pastas no controle de versão
├── .terraform.lock.hcl          # Lock file com versões exatas de provedores usados
├── main.tf                      # Arquivo principal com recursos e definições da infraestrutura
├── providers.tf                 # Configuração dos provedores (ex.: Azure, AWS, GCP)
├── terraform.tfstate            # Estado atual da infraestrutura gerenciado pelo Terraform
├── terraform.tfstate.backup     # Backup do estado anterior
├── variables.tf                 # Declaração de variáveis utilizadas nos módulos e recursos
└── versions.tf                  # Definição da versão do Terraform e restrições de provedores
````

Com esses passos, toda a construção dos recursos cloud e atribuição de funções serão realizadas.

- Necessário instalar CLI da Azure e Terraform na maquina e realizar o login com sua conta da Azure.
- Para verificar se o terraform está instalado em sua maquina, execute o seguinte comando no terminal:

`terraform --version`


OBS: A versão da imagem abaixo é necessária para executar comando para criação do recurso Databricks


<div align="center">
  <img src="https://github.com/user-attachments/assets/86edf789-0727-45a7-a633-0432e92a72b3" alt="versão terraform">
  <p><b>Versão Terraform</b></p>
</div>

<p align="left">
</p>

- Para realizar o login com sua conta, execute o seguinte comando no terminal:

`az login`

Isso irá fazer com que seja aberto uma pagina no browser para realizar a conexão e para verificar se a conexão foi realizada com sucesso execute o seguinte comando:

` az account show --output json`

- Para que os recursos sejam criados, é necessários registra-los antes com os seguintes comandos:

Databricks: `Register-AzResourceProvider -ProviderNamespace Microsoft.Databricks`

Grafana Manage: `az provider register --namespace Microsoft.Dashboard`

AZ Function: `az provider register --namespace Microsoft.Web`

StorageAccount: `az provider register --namespace Microsoft.Storage`

Após essas execuções, podemos iniciar a criação de recursos com Terraform

- Para iniciar o terraform, execute o seguinte comando no terminal:

`terraform init`

OBS: Será instalado os plugins do arquivo providers.tf

- Para validar se o terraform está com as configurações correta, execute o seguinte comando no terminal:

`terraform validate`

- Para iniciar a construção do ambiente, primeiro o Terraform precisa planejar a construção e para isso, execute o seguinte comando no terminal:

`terraform plan`

- Para aplicar ao planejamento realizado anteriormente, execute o seguinte comando no terminal:

`terraform apply -auto-approve`

Ao final da execução teremos os seguintes recursos cloud na Azure:

<div align="center">
  <img src="https://github.com/user-attachments/assets/5e9774f0-6e12-4eaa-83c8-aef829cd0022" alt="Ddasboard azure portal">
</div>

<p align="left">
</p>

- Para excluir todos os recursos do projeto, execute o seguinte comando no terminal:

`terraform destroy -auto-approvee`

## Construção do Ambiente Manualmente:

### 1.Criação do storageaccount

- Com o grupo de recursos criado, o primeiro passo foi a criação de uma conta de armazenamento Gen2 com redundância local e camada hot, pois os dados serão acessados em alta frequência por se tratar de um processo streaming near real time.
- Na mesma conta de armazenamento, foi criado um único containers.
- E para este container, foi criado um diretorio com o nome "inbound", que receberá os dados via Azure Stream Analytics:

OBS: Diferente do V1, onde tinhamos diretorio bronze, silver e gold para armazenamento de external tables e conexão com volumes, nesse projeto, optei pelo Scope do Databricks conectado com secrets e key do storageaccount armazenado em AKV.

<div align="center">
  <img src="https://github.com/user-attachments/assets/6730324c-5290-4e62-bd06-71921b779e12" alt="storageaccount">
  <p><b>StorageAccount-Estrutura Inbound</b></p>
</div>

<p align="left">
</p>

- Em gerenciamento do ciclo de vida dos blobs, foi configurado um limite de vida de 1 dia para arquivos que estão na stage inbound para que não tenhamos uma grande quantidade desnecessária de arquivos salvos na conta de armazenamento que já foram processados anteriormente.

OBS: O clico de vida de 1 dia de arquivos em stage inbound serve também para uma margem de segurança em casos de falhas no processo de extração na aplicação do Azure Function.

<div align="center">
  <img src="https://github.com/user-attachments/assets/59952bef-fcb4-44da-ad7e-0d5db95b7883" alt="ciclo de vida blobs">
  <p><b>Definindo Ciclo de Vida Blobs</b></p>
</div>

<p align="left">
</p>

### 2.Criação do Azure Function

Com o AZ Function criado, precismos criar nossa primeira aplicação e podemos fazer isso dentro do VSCode:
Instale as extensões: Azure Functions

<div align="center">
  <img src="https://github.com/user-attachments/assets/e6aa0a44-b12c-4452-9754-d0c871e284bd" alt="extensão azure fucntion">
  <p><b>Extensão Azure Function</b></p>
</div>

<p align="left">
</p>

Iinicando a criação do aplicativo, siga o passo a passo:

<div align="center">
  <img src="https://github.com/user-attachments/assets/48ef7ef4-039d-43e9-9622-9d41f1c7b572" alt="criando app">
  <p><b>Iniciando a Criação do App</b></p>
</div>

<p align="left">
</p>

<div align="center">
  <img src="https://github.com/user-attachments/assets/e53f9601-cd40-449f-b21d-12b756639339" alt="repos app">
  <p><b>Repositório do App</b></p>
</div>

<p align="left">
</p>

<div align="center">
  <img src="https://github.com/user-attachments/assets/1a102abf-2017-468e-8727-997c640263dd" alt="langue app">
  <p><b>Escolhendo linguagem do App</b></p>
</div>

<p align="left">
</p>


<div align="center">
  <img src="https://github.com/user-attachments/assets/e3c25a39-7a16-48b7-9dcc-3cb0af559ed1" alt="langue app versão">
  <p><b>Escolhendo Versão</b></p>
</div>

<p align="left">
</p>

<div align="center">
  <img src="https://github.com/user-attachments/assets/47056eeb-f9b5-447a-a8e5-5eea8b399f4f" alt="app type">
  <p><b>Tipo da Aplicação</b></p>
</div>

<p align="left">
</p>

<div align="center">
  <img src="https://github.com/user-attachments/assets/6d7cc159-86fd-431d-b71f-e9a34e8004a4" alt="app name">
  <p><b>Nome da Aplicação</b></p>
</div>

<p align="left">
</p>

<div align="center">
  <img src="https://github.com/user-attachments/assets/64341595-c5f7-4fff-8545-7259cd22da43" alt="con app">
  <p><b>Para aplicação TimeTrigger Insira o Cron da Aplicação</b></p>
</div>

<p align="left">
</p>

<div align="center">
  <img src="https://github.com/user-attachments/assets/6e9b111c-c12c-46d7-aa12-929931cb2175" alt="app criado">
  <p><b>Criação da Aplicação Finalizada</b></p>
</div>

<p align="left">
</p>

Após a estruturação e configuração do app concluída, precismos deploya a aplicação para o Azure Function

Execute o seguinte comando no terminal:

`func azure functionapp publish appnintendo --python`

<div align="center">
  <img src="https://github.com/user-attachments/assets/b0246d63-770d-4979-893a-b6279855b158" alt="app deploy">
  <p><b>Saída ao finalziar o deploy</b></p>
</div>

<p align="left">
</p>

Agora nossa aplicação estará ativa e em execução no Azure fucntion

<div align="center">
  <img src="https://github.com/user-attachments/assets/907b97e2-1383-45ae-a75f-dc7dbac172ca" alt="app az fucntion">
  <p><b>Aplicações Ativas</b></p>
</div>

<p align="left">
</p>

### 3.Criação do Event Hub

Ao criar o recruso do Event Hub na Azure, é necessário a criação de um Hub de eventos, crie algo parecido como o da imagem abaixo:

<div align="center">
  <img src="https://github.com/user-attachments/assets/6657e6df-f74b-4a03-ab5a-843d77dc2a40" alt="hub de evento">
</div>

Agora vá até "Recursos -> Processar dados" para criação de um job no Azure Stream Analytics

<div align="center">
  <img src="https://github.com/user-attachments/assets/c5164d56-a35c-45f7-af68-d8abb95a95cd" alt="stream job">
</div>

Não esqueça de configurar uma chave de politica de acesso para que o AZ Function possa enviar mensagens ao Hub de Eventos:

<div align="center">
  <img src="https://github.com/user-attachments/assets/2d72f1b5-65d6-453f-b134-bd4d87f32b7a" alt="politica de acesso">
</div>

### 4.Criação do Azure Databricks

Com o Azure Databricks criado sem nenhuma particularidade específica, basta acessar o workspace para realizar as configurações locais:
- Integrar o GitHub ao Databricks com um token de uso pessoal.

<div align="center">
  <img src="https://github.com/user-attachments/assets/572867a3-c4d6-4308-9ecd-7e6028e33297" alt="token-git">
  <p><b>Configurando Git no Databricks</b></p>
</div>

<p align="left">
</p>

- Criação de um cluster: o Standard_D4s_v3 é mais que suficiente.

<div align="center">
  <img src="https://github.com/user-attachments/assets/9cefd335-a8ed-4de3-ab76-4bd5c428f804" alt="cluster databricks">
  <p><b>Configuração do Cluster</b></p>
</div>

<p align="left">
</p>

- Importe o seguinte repositório em sua workspace:
  https://github.com/felipegoraroficial/projeto_nintendo.git

  <div align="center">
  <img src="https://github.com/user-attachments/assets/97a78d49-9ff4-414f-a175-27f54ebf0708" alt="Workspace">
  <p><b>Workspace</b></p>
</div>

<p align="left">
</p>

### 5.Liberação de System Tables

- Verifique se o seu usuário está como adimin do workspace do databricks
Para fazer isso, basta acessar o Microsfot Entry ID e ir em Funções e Administradores para verificar se seu usuário possui a função de Adminsitrador Global

<div align="center">
  <img src="https://github.com/user-attachments/assets/21ca8d5a-c4b3-4e78-bbfa-be9b7bb96a9f" alt="config system table 1">
</div>

<p align="left">
</p>

- Caso não esteja siga os passos abaixos para atribuir seu usuario como admin
Ao criar uma conta na Azure, é criado um email corporativo default, voce consegue obter esse e-mail acessando o Microsfot Entry ID em Usário

<div align="center">
  <img src="https://github.com/user-attachments/assets/993b5437-9d5a-4a65-8bb2-2a894e7f86f4" alt="config system table 2">
</div>

<p align="left">
</p>

Acesse o link https://accounts.azuredatabricks.net/ e atribua o seu email pessoal como admin global do databricks

<div align="center">
  <img src="https://github.com/user-attachments/assets/e82232ca-eddb-4a59-b8a7-1e6ca4a41d28" alt="config system table 3">
</div>

<p align="left">
</p>

- Execute o codigo, em um notebook do databricks, abaixo para verificar as system tables que estão disponiveis para adquirir ao catalogo

`curl -X GET https://<sua instance id>.azuredatabricks.net/api/2.0/unity-catalog/metastores/<seu metastore id>/systemschemas \
  -H "Authorization: Bearer <seu token>"`

- Execute o codigo, em um notebook do databricks, abaixo para anexar a tabela ao catalogo

`curl -v -X PUT -H "Authorization: Bearer <seu token>" "https://<sua instance id>.azuredatabricks.net/api/2.0/unity-catalog/metastores/<seu metastore id>/systemschemas/<nome da tabela>"`

- Caso ainda precise de ajuda, a documentação abaixo pode te instruir:

https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-privileges/admin-privileges#assign-metastore-admin

### 6.Workflow Databricks

- Crie um Pipeline com DLT

<div align="center">
  <img src="https://github.com/user-attachments/assets/64306677-5a3f-4338-9ef0-9541faa76b2d" alt="DLT jobs">
</div>

<p align="left">
</p>

- Após a configuração, o databricks irá iniciar a criação do Pipeline:

<div align="center">
  <img src="https://github.com/user-attachments/assets/afcc3716-065b-43df-8463-f32f95c391f4" alt="DLT config">
</div>

<p align="left">
</p>

- Crie um workflows: inclua tags e descrição se preferir. É muito util incluir tags e descrição para identificação de Jobs quandos e trata de um ambiente com diversos Jobs criado.

- Adicione a seguinte Agenda ao Job: Chegada do ficheiro. Este tipo de ativação do Job faz com que o start do fluxo se inicie a partir de novos arquivos que são adicionados em um local especificado

<div align="center">
  <img src="https://github.com/user-attachments/assets/56ee602f-3523-49b7-9c1c-918167b4293d" alt="triggers jobs">
</div>

<p align="left">
</p>

- Para receber alertas de Jobs que falharam ou que tiveram um tempo de execução maior do que o esperado, use a seguinte configuração:

<div align="center">
  <img src="https://github.com/user-attachments/assets/255caed3-91b4-4eb2-9f88-a2e6780f1c2e" alt="triggers jobs">
</div>

<p align="left">
</p>

<div align="center">
  <img src="https://github.com/user-attachments/assets/b743d41b-0998-47a4-83c1-c78041f00c33" alt="triggers jobs">
</div>

<p align="left">
</p>

- Teremos um pipeline parecido com o da imagem abaixo:

<div align="center">
  <img src="https://github.com/user-attachments/assets/1c68cd3a-fc1a-4a1f-8e2e-fd0be7c58c58" alt="workflows medalion">
  <p><b>Medalion Workflows do Databricks</b></p>
</div>

- Para a execução dos Jobs foi utilizado um cluster de jobs afim de minimizar consumo de DBUs com o cluster de uso pessoal:

<div align="center">
  <img src="https://github.com/user-attachments/assets/6b3fd527-6199-4251-95aa-74c7ed562f9d" alt="job cluster">
  <p><b>Job Cluster no Databricks</b></p>
</div>

OBS: Se preferir, poderá ser executado o notebook "pipeline" na pasta src/config que irá criar tanto o pipeline DLT quanto o Job

### 7.Monitoramento Grafna

- Conexão Databricks com Grafana:
Utilziando o Cluster Serverless Starter Warehouse

<div align="center">
  <img src="https://github.com/user-attachments/assets/b15df42b-9df3-4e4c-8218-f4af13de4858" alt="sql cluster">
  <p><b>Serverless Warehouse</b></p>
</div>

Conectando cluster ao Grafana

OBS: Necessário gerar um Token

<div align="center">
  <img src="https://github.com/user-attachments/assets/dcc2b314-4290-454c-b25a-4a6c39888d4b" alt="conection">
  <p><b>Conectando Grafna Databricks</b></p>
</div>

Teremos um dashboard de monitoramento:

  <div align="center">
  <img src="https://github.com/user-attachments/assets/49d4a589-9856-45cf-ad31-d4ecfc7b9110" alt="databricks monitoring">
  <p><b>Monitoramento Grafna Databricks</b></p>
</div>

- Conexão Azure Monitor com Grafana:

Para a criação do moledos ao Azure Monitor foi integrado templates já existentes no Grafana com o Applications Insigths:

link: https://grafana.com/grafana/dashboards/

OBS: Para conectar o Az Monitor ao Grafana, é necessário a criação de um Registro de Aplicativo

  <div align="center">
  <img src="https://github.com/user-attachments/assets/ad72034a-1638-43bb-9d97-36b6c8ab6483" alt="coenxão az monitor">
  <p><b>Conexão AZ Monitor</b></p>
</div>

Após importar o dashboard da sua prefêrencia, teremos um dashboards iguala  este:

  <div align="center">
  <img src="https://github.com/user-attachments/assets/6660d754-eb2c-4710-8869-96d1963bf830" alt="applications insigths">
  <p><b>Dashboards Azure Function</b></p>
</div>

  <div align="center">
  <img src="https://github.com/user-attachments/assets/293a62f0-0b40-48b7-a075-d89867a82d3a" alt="event hub">
  <p><b>Dashboards Event Hub</b></p>
</div>

  <div align="center">
  <img src="https://github.com/user-attachments/assets/07d4bba4-01d6-43f0-b1a2-6c67cad65fe8" alt="storage monitoring">
  <p><b>Monitoramento de StorageAccount</b></p>
</div>


  
