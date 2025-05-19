# WebScrapy de Anúncios de Dados de Consoles Nintendo Switch

## Objetivo:

O objetivo deste projeto é a captura de dados quase em tempo real referentes a anúncios de vendas em e-commerce/marketplaces de forma escalável, com foco específico nos consoles Nintendo Switch. Os dados capturados são tratados e armazenados em um ambiente na nuvem, permitindo sua integração com ferramentas de visualização de dados (DataViz). Isso possibilita a geração de gráficos e insights para identificar os melhores preços e as principais características dos consoles em diferentes marketplaces.

<div align="center">
  <img src="https://github.com/user-attachments/assets/44df77ae-63ca-4d69-8c58-7820cdb98b5a" alt="Desenho Técnico">
  <p><b>Desenho Funcional</b></p>
</div>

<p align="left">
</p>

## Introdução:

O projeto foi desenvolvido utilizando as linguagens de programação Python e PySpark na paltaforma de nuvem da Azure. Para a normalização e modelagem de dados, foi utilizado SQL na plataforma dbt Cloud.
Usando Azure Functions para realizar requests em quase tempo real e armazenar o conteuúdo html no diretorio inbound da conta de armazenamento da Azure.
Com Azure Databricks como plataforma do processo de ELT, foi utilizando a linguegam Pyspark apra lidar com grandes volume de dados na stage silver e gold
Para tornar o projeto escalavél, utilizamos a biblioteca langchain para utilização de IA para captura de contéudos html em cada requisição feita pelo app Azure Functions.
Por fim, os dados são processados e carregados em uma tabela externa com o proposito de facilitar a moigração de dados para outra plataforma, caso necessário.

No dbt, é feita a conexão do catálogo do Databricks e são criadas tables de normalização de dados e views de métricas para análise de dados.

Todo o processo ocorre no workflow do Databricks de forma agendada, com alertas enviados por e-mail em caso de tempo de processo ultrapassar o limite estimado ou falha.

No final do workflow é gerado dados referente a log do processo e lienage de tabelas que são armazenado em uma tabela no catalogo do databricks que servirão de fonte de dados ao dashboard de monitoramento do workspace.

Os scripts são versionados e podem ser separados por ambientes como por exemplo: desenvolvimento (dev) e produção (prd).

<div align="center">
  <img src="https://github.com/user-attachments/assets/74d30a1c-222a-4576-bb07-a0a2ecaa7be9" alt="Desenho Técnico">
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
    - **Benefício**: Flexibilidade para processo prdotivo com menor probabilidade para correções de bugs.

3. **Processamento e Escalabilidade para BigData**:
    - **Objetivo**: Aplicar processamento Spark ao conjuntos de dados na etapa de transformação.
    - **Benefício**: Melhoria da qualidade dos dados, com correção de dados e padronização do mesmo e, além disso, o processamento de dados escalonáveis e suportando big data.

4. **Independência entre Plataformas**:
    - **Objetivo**: Armazenar dados tratados em uma external table.
    - **Benefício**: Garantir a flexibilidade para migrações de dados para outras plataformas.

5. **Separação de Plataforma entre Times**:
    - **Objetivo**: Separação de processos entre engenheiros de dados (Plataforma Databricks) e analistas dados (Plataforma dbt).
    - **Benefício**: Ganho em foco e agilidade no processo, segurança em carga de trabalho e governança sobre a plataforma

6. **Monitoramento de Processo**:
    - **Objetivo**: Integrar ambas plataforma em um unico pipeline de dados.
    - **Benefício**: Monitoramento e velocidade na interpretação de incidente para atuar em correções e/ou manutenções.

## Construção do projeto:

### 1.Criação do storageaccount

- Com o grupo de recursos criado, o primeiro passo foi a criação de uma conta de armazenamento Gen2 com redundância local e camada cool, pois os dados serão acessados com pouca frequência por se tratar de um processo batch.
- Na mesma conta de armazenamento, foram criados dois containers, dev e prd, para separar os dados de produção daqueles em desenvolvimento.
- Em cada container, foram criados volumes do Databricks com link externos para definição de uma hierarquia de pastas que será utilizada para a construção do processo ELTL no modelo de medalhão, onde temos os seguintes dados:
    - Inbound: Dados brutos conforme vêm da extração web em formato HTML, separados pela data da extração.
    - Bronze: Identificação dos elementos web necessários para o projeto e armazenados em um arquivo PARQUET conforme a data do arquivo de extração.
    - Silver: União de todos os arquivos em seus diferentes diretórios e o tratamento de limpeza e ajuste dos dados.
    - Gold: Criação da external table no databricks com a fonte de dados na conta de armazenamento particionada pela data de extração.

<div align="center">
  <img src="https://github.com/user-attachments/assets/c064337e-660e-4664-b34f-f2f2ccbbb99f" alt="storageaccount">
  <p><b>StorageAccount-Estrutura</b></p>
</div>

<p align="left">
</p>

- Em gerenciamento do ciclo de vida dos blobs, foi configurado um limite de vida de 30 dias para arquivos que estão an stage inbound e bronze para que não tenhamos uma grande quantidade de arquivos salvos na conta de armazenamento já que os registros são armazenados em external tables do catalog do databricks.

OBS: O clico de vida de 30 dias de arquivos em stage inbound e bronze serve também para uma margem de segurança em casos de alterações de elementos do html extraidos para interação com o BeautifulSoup.

<div align="center">
  <img src="https://github.com/user-attachments/assets/6d89a267-6f16-4940-abea-aed50aad7ef7" alt="ciclo de vida blobs">
  <p><b>Definindo Ciclo de Vida Blobs</b></p>
</div>

<p align="left">
</p>

### 2.Criação do Azure Databricks

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

- Importe o reseguinte repositório em sua workspace:
  https://github.com/felipegoraroficial/projeto_nintendo.git

  <div align="center">
  <img src="https://github.com/user-attachments/assets/2e0668da-a588-473e-af56-1864db730d82" alt="Workspace">
  <p><b>Workspace</b></p>
</div>

<p align="left">
</p>
  

- Criação da external table no cátalogo.

<div align="center">
  <img src="https://github.com/user-attachments/assets/87fa2d19-d802-4f03-befc-940b321fbc24" alt="external table gold">
  <p><b>External Table</b></p>
</div>

<p align="left">
</p>

- Criação de volumes  no cátalogo.

<div align="center">
  <img src="https://github.com/user-attachments/assets/455722a8-8466-4e08-9e8c-6f86377bd2e7" alt="volumes databricks">
  <p><b>Criação do Volumes no Databricks</b></p>
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
- Criação de dois external location para os container dev e prd (necessário para a criação da external table e leituras e gravações de dados).

<div align="center">
  <img src="https://github.com/user-attachments/assets/7314c14e-a878-41a3-a4de-2d63fb470bc0" alt="config-access-conector">
</div>

<p align="left">
</p>
  
### 6.Conexão entre dbt e databricks

- Com a conta no dbt criada, crie seu projeto.
- Conecte o Databricks ao dbt utilizando as informações do cluster: o host, o caminho HTTP (http path) e a porta, que geralmente é a 443.

<div align="center">
  <img src="https://github.com/user-attachments/assets/9c22d837-4467-4c0c-bf3b-8e13acd3c683" alt="dbt conection">
  <p><b>Conectando dbt ao Databricks</b></p>
</div>

<p align="left">
</p>

- Conecte o GitHub ao dbt com um token de uso pessoal.
- Use este link para clonar o repositório do projeto dbt: https://github.com/felipegoraroficial/dbt_project
- Com as conexões realizadas, crie um arquivos YML para os schemas dev e prd em cada branch, que serão as fontes de busca para trabalhar com os dados no Databricks de forma separada por ambiente.
- Ao final, teremos um pipeline de dado no dbt igual o da imagem abaixo

<div align="center">
  <img src="https://github.com/user-attachments/assets/ebe099b3-0a17-4a2d-a6ec-f757f5899d3c" alt="dbt lineage">
  <p><b>Fluxo de Processo dbt</b></p>
</div>

<p align="left">
</p>

### 7.Workflow Databricks

- Crie dois workflows: um com a tag hml, que se refere ao fluxo de teste, e outro com a tag prd, que será o fluxo de produção.

<div align="center">
  <img src="https://github.com/user-attachments/assets/aa100607-25ee-4ecb-8143-d66ea889b251" alt="jobs databricks">
  <p><b>Jobs Databricks</b></p>
</div>

<p align="left">
</p>

- Em cada workflow, habilite a integração com o Git para poder rodar processos do dbt.

<div align="center">
  <img src="https://github.com/user-attachments/assets/4e913064-fc1f-4518-a391-fa8221b9ad50" alt="config-dbt-workflow-databricks">
  <p><b>Configurando Task com dbt</b></p>
</div>

<p align="left">
</p>

- As tarefas que não forem relacionadas a processos do dbt serão realizadas com notebooks do espaço local, tornando dinâmica a tratativa entre os ambientes.
- Agende o workflow de sua preferência. Para cada ambiente, foi utilizada a sintaxe cron:
  - dev: 0 0 8 ? * MON-FRI *
  - prd: 0 0 9,13,17 ? * MON-FRI *
- Ative as notificações de falhas e tempo de processo para que você seja notificado por e-mail.

<div align="center">
  <img src="https://github.com/user-attachments/assets/2a2586db-50fd-4b68-aaaf-ac0f9e7422fe" alt="details-workflow">
  <p><b>Detalhes da Configuração do Job</b></p>
</div>

<p align="left">
</p>

### 8.Dashboard Monitoramento via Databricks

- Crie um painel com as fontes de dados da tabela log-table em ambos os schemas.

Ao fim do pipeline é gerado uma tabela de log do workflow para cada ambiente, sendo dev e prd, a partir da extração de daods utilziando a API do Databricks.

Basta unir as duas tabelas para gerar uma visão de logs em ambos ambientes.

<div align="center">
  <img src="https://github.com/user-attachments/assets/db5f2d29-086c-4ef1-9e49-58546d1996d9" alt="fonte-dados-painel">
</div>

<p align="left">
</p>

- Crie um painel com as fontes de dados da tabela lineage-tables-monitoring em ambos os schemas e inclua uma coluna de contagem para criação de insights no painel do dashbaord.

<div align="center">
  <img src="https://github.com/user-attachments/assets/f9c9c445-44ef-4acc-9277-26de13890d1a" alt="query painel">
</div>

<p align="left">
</p>


- Ao final da construção e execuções do workflow, teremo resultados semelhantes ao das imagens abaixo:

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


<div align="center">
  <img src="https://github.com/user-attachments/assets/93fa08db-d369-40c1-9c37-2321646efdcb" alt="workflows">
  <p><b>Workflows do Databricks</b></p>
</div>

<p align="left">
</p>

<div align="center">
  <img src="https://github.com/user-attachments/assets/0eba13a8-e0cb-43a6-b577-18f7f0b3eb3d" alt="details workflows">
  <p><b>Execuções Workflows do Databricks</b></p>
</div>

<p align="left">
</p>
  
