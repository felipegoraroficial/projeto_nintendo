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

Usando Azure Functions para realizar requests em quase tempo real e utilizando scrap com auxilio de um agent IA para obter os conteúdos html necessários, armazenamos os dados no diretorio inbound da conta de armazenamento da Azure no formato json.

Com Azure Databricks como plataforma de pipeline de dados na arquitetura medalhão, foi utilizando a linguegam Pyspark para lidar com grandes volume de dados na stage bronze, silver e gold e SparkSQL para criação de analise de dados e BI.

Foi criado um Job dentro da plataforma Databricks que está schedulado sempre que um novo arquivo no diretorio inbound no container da StorageAccount é adicionado.

Para o monitoramento desses jobs na paltaforma do Databricks, foi criado um painel utilizando Cluster Serveless do Data Warehouse conectado a tabelas da System Tables.
Para monitoramento da Aplicação no Azure Function foi utilizando o App Insights para monitorar os logs da aplicação.

<div align="center">
  <img src="https://github.com/user-attachments/assets/fb0c3732-0456-40a4-af88-67646ce63654" alt="Desenho Técnico">
  <p><b>Desenho Técnico</b></p>
</div>

<p align="left">
</p>


## Meta:

1. **Captação de Dados Brutos em Quase Tempo Real**:
    - **Objetivo**: Criar uma aplicação no Azure Functions para capturar dados brutos diários de sites de e-commerce e marketplaces para análise posterior.
    - **Benefício**: Permite a captura de dados em quase tempo real com menor custo.

2. **Escalando Captura de Elemento HTML**:
    - **Objetivo**: Utilizar modelos de IA para extrair informações essenciais dos arquivos HTML, como links, títulos, preços, promoções, parcelamentos e imagens dos produtos.
    - **Benefício**: Escalabilidade para ampliar o leque de fonte de dados e flexibilidade para processo produtivo com menor probabilidade para correções de bugs.

3. **Processamento e Escalabilidade para BigData**:
    - **Objetivo**: Aplicar processamento Spark ao conjuntos de dados na arquitetura medalhão do pipeline.
    - **Benefício**: Melhoria da qualidade dos dados, com correção de dados e padronização do mesmo e, além disso, o processamento de dados escalonáveis e suportando big data.

4. **Data Lakehouse Estrutura Medalhão de Dados**:
    - **Objetivo**: Transformar o datalake (storageaccount) em um data lakehouse.
    - **Benefício**: Combinando a flexibilidade e escalabilidade de um data lake com a confiabilidade e performance de um data warehouse.

5. **Otimizando o processo para análise e BI**:
    - **Objetivo**: Utilização do Spark SQL no Databricks para a criação de tabelas fato e dimensão a partir de uma tabela Gold do Data Lakehouse.
    - **Benefício**: Permite uma modelagem de dados analíticos eficiente e confiável, combinando a familiaridade do SQL com a escalabilidade do Spark e a integridade do Delta Lake.

6. **Monitoramento de Processos**:
    - **Objetivo**: Monitoramento nativo da Plataforma Databricks e App Insights do Azure Function.
    - **Benefício**: Monitoramento e velocidade na interpretação de incidente para atuar em correções e/ou manutenções.

## Construção do Ambiente com Terraform:

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

<div align="center">
  <img src="https://github.com/user-attachments/assets/a2f37258-f07e-4826-9871-35d9036bc1ad" alt="iniciando terraform">
  <p><b>Iniciando Terraform</b></p>
</div>

<p align="left">
</p>

- Para validar se o terraform está com as configurações correta, execute o seguinte comando no terminal:
`terraform validate`

<div align="center">
  <img src="https://github.com/user-attachments/assets/288c8dcc-07cb-489d-b70f-3ed91b6ca601" alt="validate terraform">
  <p><b>Validando Configurações Terraform</b></p>
</div>

<p align="left">
</p>

- Para iniciar a construção do ambiente, primeiro o Terraform precisa planejar a construção e para isso, execute o seguinte comando no terminal:
`terraform plan`

<div align="center">
  <img src="https://github.com/user-attachments/assets/0d4931e9-3d47-423e-9a29-03bf8530c3f7" alt="plan terraform">
  <p><b>Planejamento Terraform</b></p>
</div>

<p align="left">
</p>

- Para aplicar ao planejamento realizado anteriormente, execute o seguinte comando no terminal:
`terraform apply`
- O Terraform irá perguntar se desejamos seguir com a aplciação, absta inserir "yes".

<div align="center">
  <img src="https://github.com/user-attachments/assets/ef8360ba-c732-4ec3-b53b-ba0c90f79a25" alt="aprove apply">
  <p><b>Aprovando Apply</b></p>
</div>

<p align="left">
</p>

<div align="center">
  <img src="https://github.com/user-attachments/assets/f22819ae-5173-4bbc-8e7a-8b54d0edb1e5" alt="terraform apply">
  <p><b>Finanlizando Construção</b></p>
</div>

<p align="left">
</p>

- Por fim, teremos o seginte recursos criados:

<div align="center">
  <img src="https://github.com/user-attachments/assets/8d45f41a-50d6-4406-96cf-c802ae00a1fd" alt="terraform criado">
  <p><b>Recursos Cloud Criado</b></p>
</div>

<p align="left">
</p>

## Construção do Ambiente Manualmente:

### 1.Criação do storageaccount

- Com o grupo de recursos criado, o primeiro passo foi a criação de uma conta de armazenamento Gen2 com redundância local e camada hot, pois os dados serão acessados em alta frequência por se tratar de um processo streaming near real time.
- Na mesma conta de armazenamento, foi criado um único containers.
- E para este container, foram criados volumes do Databricks com link externos para definição de uma hierarquia de pastas que será utilizada para a construção do pipeline de dados no modelo de arquitetura medalhão, onde temos os seguintes dados:
    - Inbound: Dados brutos conforme vêm aplucação do azure fucntion no formato json.
    - Bronze: Estruturação e processamento dos dados json que serão salvos na camada bronze de forma incremental pelo particionamento da data de extração dos dados.
    - Silver: Etapa em que os dados são padronizados, estruturados pela definição de schemas, limpeza de valores nulos entre outras etapas definidas na camada silver para garantir a maior confiabilidade dos dados brutos.
    - Gold: Ultima etapa do pipeline de dados, aqui se aplica a construção da regra de negocio que servirá de auxlio para criação de metricas e indicadores dos dados.

<div align="center">
  <img src="https://github.com/user-attachments/assets/40f51823-3c1c-4f6a-9afe-c099aff11b19" alt="storageaccount">
  <p><b>StorageAccount-Estrutura</b></p>
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



### 3.Criação do Azure Databricks

Com o Azure Databricks criado sem nenhuma particularidade específica, basta acessar o workspace para realizar as configurações locais:
- Integrar o GitHub ao Databricks com um token de uso pessoal.

<div align="center">
  <img src="https://github.com/user-attachments/assets/572867a3-c4d6-4308-9ecd-7e6028e33297" alt="token-git">
  <p><b>Configurando Git no Databricks</b></p>
</div>

<p align="left">
</p>

- Criação de um cluster: o Standard_DS3_v2 é mais que suficiente.

<div align="center">
  <img src="https://github.com/user-attachments/assets/c7cd3457-fa4d-4f57-a816-43775197e5a1" alt="cluster databricks">
  <p><b>Configuração do Cluster</b></p>
</div>

<p align="left">
</p>

- Importe o seguinte repositório em sua workspace:
  https://github.com/felipegoraroficial/projeto_nintendo.git

  <div align="center">
  <img src="https://github.com/user-attachments/assets/6a0d0f95-6dea-4ea5-b055-4ccccf2ab823" alt="Workspace">
  <p><b>Workspace</b></p>
</div>

<p align="left">
</p>

### 4.Liberação de System Tables

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

### 5.Criação de um Acess Conector

<div align="center">
  <img src="https://github.com/user-attachments/assets/be35dabc-4ecf-4b86-bc1b-544c3b719fa1" alt="access-conector">
</div>

<p align="left">
</p>

- Crie um Acess Conector com a mesma região e grupo de recurso do projeto.
- Atribua a função de Colaborador de Dados do Storage Blob ao acess conector.
- Criação de uma credencial externa no workspace do azure databricks.
- Criação de dois external location para os container dev e prd (necessário para a criação dos volumes para leituras e gravações de dados).

<div align="center">
  <img src="https://github.com/user-attachments/assets/7314c14e-a878-41a3-a4de-2d63fb470bc0" alt="config-access-conector">
</div>

<p align="left">
</p>

Com o access conector configurado ao storageaccount e as credenciais e external location criadas em nosso workspace, agora podemos criar os volumes conectados aos diretorios inbound, bronze, silver e gold do container nintendo. Seu catalogo deve estar parecido com o da imagem abaixo:

<div align="center">
  <img src="https://github.com/user-attachments/assets/bac9a97c-deff-4bec-9ba8-f8f0d55e4b04" alt="catalogo databricks">
</div>

<p align="left">
</p>



### 6.Workflow Databricks

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
  <img src="https://github.com/user-attachments/assets/93fa08db-d369-40c1-9c37-2321646efdcb" alt="workflows">
  <p><b>Workflows do Databricks</b></p>
</div>


### 7.Monitoramento Aplicação

- Com aplication Insghts podemos verificar os logs da aplicação e definir alertas para receber aviso em caso de falha da aplicação ou referente a uma condição especifica:

<div align="center">
  <img src="https://github.com/user-attachments/assets/fd99f0e7-b18d-41e7-abac-cf2f4cbdeed0" alt="triggers jobs">
</div>

### 8.Dashboard Monitoramento via Databricks

<div align="center">
  <img src="https://github.com/user-attachments/assets/99bb03ff-c23f-4215-936b-54ad73388899" alt="dashboard-monitoramento">
  <p><b>Dashboard de Monitoramento de Jobs no Databricks</b></p>
</div>

<p align="left">
</p>

<div align="center">
  <img src="https://github.com/user-attachments/assets/98165ea3-e252-462a-8e15-7620cc1dee93" alt="dashboard-monitoramento">
  <p><b>Dashboard de Monitoramente de Leitura e Escrita das Tabelas no Databricks</b></p>
</div>

<p align="left">
</p>


  
