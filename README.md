# WebScrapy de Anúncios de Dados de Consoles Nintendo Switch

## Introdução:

O projeto foi desenvolvido utilizando as linguagens de programação Python e PySpark no ambiente em nuvem do Azure Databricks. Para a normalização e modelagem de dados, foi utilizado SQL na plataforma dbt Cloud.

A arquitetura do projeto envolve recursos da Azure integrados ao dbt Cloud. Usando o Databricks como plataforma do processo de ELT, os dados extraídos da web são armazenados em um diretório inbound dentro de um contêiner da conta de armazenamento da Azure com a data de extração. Utilizamos o BeautifulSoup para identificar elementos e carregar informações na stage bronze. Com PySpark, carregamos todos os dados da stage bronze e passamos por uma limpeza e transformação de dados até o carregamento dos dados tratados em uma stage silver. Por fim, os dados são processados e carregados em uma tabela externa que está particionada pela data de extração.

No dbt, é feita a conexão do catálogo do Databricks e são criadas tables de normalização de dados e views de métricas para análise de dados.

Todo o processo ocorre no workflow do Databricks de forma agendada, com alertas enviados por e-mail em caso de tempo de processo ultrapassar o limite estimado ou falha.

No final do workflow é gerado dados referente a log do processo e lienage de tabelas que são armazenado em uma tabela no catalogo do databricks que servirão de fonte de dados ao dashboard de monitoramento do workspace.

Os scripts são versionados e separados por ambientes de desenvolvimento (dev) e produção (prd).

![arquitetura-projeto-nintendo](https://github.com/user-attachments/assets/7e06bcbe-da5e-42a4-a9d2-bf7abaf7a238)

## Meta:

1. **Captação e Armazenamento de Dados Brutos**:
    - **Objetivo**: Capturar dados brutos diários de sites de e-commerce e marketplaces para análise posterior.
    - **Benefício**: Permite a reprocessamento dos dados caso os sites mudem, preservando a integridade dos dados históricos.

2. **Captação e Armazenamento de Elemento HTML**:
    - **Objetivo**: Extrair informações essenciais dos arquivos HTML, como links, títulos, preços, promoções, parcelamentos e imagens dos produtos.
    - **Benefício**: Flexibilidade para ajustar o processamento conforme necessário, sem a perda de dados brutos.

3. **Processamento e Escalabilidade para BigData**:
    - **Objetivo**: Aplicar processamento Spark ao conjuntos de dados na etapa de transformação.
    - **Benefício**: Melhoria da qualidade dos dados, com correção de dados e padronização do mesmo e, além disso, o processamento de dados escalonáveis e suportando big data.

4. **Armazenamento Seguro dos Dados**:
    - **Objetivo**: Armazenar dados tratados em uma external table no Databricks, garantindo a segurança e integridade dos dados.
    - **Benefício**: Proteção dos dados em armazenamento externo, prevenindo perdas ou alterações equivocadas no dado.

![external-table-databricks](https://github.com/user-attachments/assets/87fa2d19-d802-4f03-befc-940b321fbc24)

5. **Separação de Plataforma entre Times**:
    - **Objetivo**: Separação de processos entre engenheiros de dados (Plataforma Databricks) e analistas dados (Plataforma dbt).
    - **Benefício**: Ganho em foco e agilidade no processo, segurança em carga de trabalho e governança sobre a plataforma

![lineage-dbt](https://github.com/user-attachments/assets/ebe099b3-0a17-4a2d-a6ec-f757f5899d3c)

6. **Monitoramento de Processo**:
    - **Objetivo**: Integrar ambas plataforma em um unico pipeline de dados.
    - **Benefício**: Monitoramento e velocidade na interpretação de incidente para atuar em correções e/ou manutenções.

![dashboard-monitoramento](https://github.com/user-attachments/assets/99bb03ff-c23f-4215-936b-54ad73388899)

![Image](https://github.com/user-attachments/assets/98165ea3-e252-462a-8e15-7620cc1dee93)

![Image](https://github.com/user-attachments/assets/93fa08db-d369-40c1-9c37-2321646efdcb)

![execucoes-job](https://github.com/user-attachments/assets/0eba13a8-e0cb-43a6-b577-18f7f0b3eb3d)

## Etapas do projeto:

### 1.Criação do storageaccount

- Com o grupo de recursos criado, o primeiro passo foi a criação de uma conta de armazenamento Gen2 com redundância local e camada cool, pois os dados serão acessados com pouca frequência por se tratar de um processo batch.
- Na mesma conta de armazenamento, foram criados dois containers, dev e prd, para separar os dados de produção daqueles em desenvolvimento.
- Em cada container, foram criados volumes do Databricks com link externos para definição de uma hierarquia de pastas que será utilizada para a construção do processo ELTL no modelo de medalhão, onde temos os seguintes dados:
    - Inbound: Dados brutos conforme vêm da extração web em formato HTML, separados pela data da extração.
    - Bronze: Identificação dos elementos web necessários para o projeto e armazenados em um arquivo JSON conforme a data do arquivo de extração.
    - Silver: União de todos os arquivos em seus diferentes diretórios e o tratamento de limpeza e ajuste dos dados.
    - Gold: Criação da external table no databricks com a fonte de dados na conta de armazenamento particionada pela data de extração.

  ![storage-container](https://github.com/user-attachments/assets/c064337e-660e-4664-b34f-f2f2ccbbb99f)

- Em gerenciamento do ciclo de vida dos blobs, foi configurado um limite de vida de 30 dias para arquivos que estão an stage inbound e bronze para que não tenhamos uma grande quantidade de arquivos salvos na conta de armazenamento já que os registros são armazenados em external tables do catalog do databricks.

OBS: O clico de vida de 30 dias de arquivos em stage inbound e bronze serve também para uma margem de segurança em casos de alterações de elementos do html extraidos para interação com o BeautifulSoup.

  ![Image](https://github.com/user-attachments/assets/6d89a267-6f16-4940-abea-aed50aad7ef7)

### 2.Criação do Azure Databricks

Com o Azure Databricks criado sem nenhuma particularidade específica, basta acessar o workspace para realizar as configurações locais:
- Integrar o GitHub ao Databricks com um token de uso pessoal.

![token-git](https://github.com/user-attachments/assets/572867a3-c4d6-4308-9ecd-7e6028e33297)

- Criação de um cluster: o Standard_DS3_v2 é mais que suficiente.

![cluster-databricks](https://github.com/user-attachments/assets/c7cd3457-fa4d-4f57-a816-43775197e5a1)

- Criação de schemas Dev e Prd no Catálogo do Databricks para separar os dados em ambientes.

![catalog-databricks](https://github.com/user-attachments/assets/0d2f82cd-e585-4161-a853-2ac4396dc037)

- No espaço de trabalho, crie duas pastas, dev e prd, para separar os códigos em cada branch.
- Importe os reseguintes repositórios para cada pasta com suas respectivas branches:
  https://github.com/felipegoraroficial/projeto_nintendo.git (o repositório do projeto)
  https://github.com/felipegoraroficial/meus_scripts_pyspark.git (o repositório referente a funções pypark para tratativas de dados do projeto)
  https://github.com/felipegoraroficial/meus_scripts_pytest.git (o repositório referente a testes de dados do projeto)
  
![workspace - databricks](https://github.com/user-attachments/assets/68fc0f43-25e4-4d7a-979c-9b49ccb5b038)
  
- Criação de uma credencial externa no cálogo.
- Criação de duas external location para os container dev e prd (necessário para a criação da external table e volumes).
- Criação de volumes, em ambos schmas dev e prd, e para cada hierarquia medalhão mencionada na etapa acima.

![volume-databricks](https://github.com/user-attachments/assets/455722a8-8466-4e08-9e8c-6f86377bd2e7)

### 4.Liberação de System Tables

- Verifique se o seu usuário está como adimin do workspace do databricks
Para fazer isso, basta acessar o Microsfot Entry ID e ir em Funções e Administradores para verificar se seu usuário possui a função de Adminsitrador Global

![Image](https://github.com/user-attachments/assets/21ca8d5a-c4b3-4e78-bbfa-be9b7bb96a9f)

- Caso não esteja siga os passos abaixos para atribuir seu usuario como admin
Ao criar uma conta na Azure, é criado um email corporativo default, voce consegue obter esse e-mail acessando o Microsfot Entry ID em Usário

![Image](https://github.com/user-attachments/assets/993b5437-9d5a-4a65-8bb2-2a894e7f86f4)

Acesse o link https://accounts.azuredatabricks.net/ e atribua o seu email pessoal como admin global do databricks

![Image](https://github.com/user-attachments/assets/e82232ca-eddb-4a59-b8a7-1e6ca4a41d28)

- Execute o codigo, em um notebook do databricks, abaixo para verificar as system tables que estão disponiveis para adquirir ao catalogo

`curl -X GET https://<sua instance id>.azuredatabricks.net/api/2.0/unity-catalog/metastores/<seu metastore id>/systemschemas \
  -H "Authorization: Bearer <seu token>"`

- Execute o codigo, em um notebook do databricks, abaixo para anexar a tabela ao catalogo

`curl -v -X PUT -H "Authorization: Bearer <seu token>" "https://<sua instance id>.azuredatabricks.net/api/2.0/unity-catalog/metastores/<seu metastore id>/systemschemas/<nome da tabela>"`

- Caso ainda precise de ajuda, a documentação abaixo pode te instruir:

https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-privileges/admin-privileges#assign-metastore-admin

### 5.Criação de um Acess Conector

- Crie um Acess Conector com a mesma região e grupo de recurso do projeto.
- Atribua a função de Colaborador de Dados do Storage Blob ao acess conector.
- Criação de uma credencial externa no workspace do azure databricks.
- Criação de dois external location para os container dev e prd (necessário para a criação da external table e leituras e gravações de dados).
  
### 6.Conexão entre dbt e databricks

- Com a conta no dbt criada, crie seu projeto.
- Conecte o Databricks ao dbt utilizando as informações do cluster: o host, o caminho HTTP (http path) e a porta, que geralmente é a 443.
- Conecte o GitHub ao dbt com um token de uso pessoal.
- Com as conexões realizadas, crie dois arquivos YML para os schemas dev e prd, que serão as fontes de busca para trabalhar com os dados no Databricks.

![dbt-conection](https://github.com/user-attachments/assets/9c22d837-4467-4c0c-bf3b-8e13acd3c683)

### 7.Workflow Databricks

- Crie dois workflows: um com a tag hml, que se refere ao fluxo de teste, e outro com a tag prd, que será o fluxo de produção.
  
![workflows](https://github.com/user-attachments/assets/aa100607-25ee-4ecb-8143-d66ea889b251)

- Em cada workflow, habilite a integração com o Git para poder rodar processos do dbt.

![config-dbt-workflow-databricks](https://github.com/user-attachments/assets/4e913064-fc1f-4518-a391-fa8221b9ad50)

- As tarefas que não forem relacionadas a processos do dbt serão realizadas com notebooks do espaço local, tornando dinâmica a tratativa entre os ambientes.
- Agende o workflow de sua preferência. Para cada ambiente, foi utilizada a sintaxe cron:
  - dev: 0 0 8 ? * MON-FRI *
  - prd: 0 0 9,13,17 ? * MON-FRI *
- Ative as notificações de falhas e tempo de processo para que você seja notificado por e-mail.

![details-workflow](https://github.com/user-attachments/assets/2a2586db-50fd-4b68-aaaf-ac0f9e7422fe)

### 8.Dashboard Monitoramento via Databricks

- Crie um painel com as fontes de dados da tabela log-table em ambos os schemas.

Ao fim do pipeline é gerado uma tabela de log do workflow para cada ambiente, sendo dev e prd, a partir da extração de daods utilziando a API do Databricks.

Basta unir as duas tabelas para gerar uma visão de logs em ambos ambientes.

![fonte-dados-painel](https://github.com/user-attachments/assets/db5f2d29-086c-4ef1-9e49-58546d1996d9)

- Crie um painel com as fontes de dados da tabela lineage-tables-monitoring em ambos os schemas e inclua uma coluna de contagem para criação de insights no painel do dashbaord.

![Image](https://github.com/user-attachments/assets/f9c9c445-44ef-4acc-9277-26de13890d1a)
  
