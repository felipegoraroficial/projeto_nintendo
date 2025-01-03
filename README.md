# WebScrapy de Dados de Anúncios de Consoles Nintendo Switch

## Introdução:

O projeto foi desenvolvido utilizando as linguagens de programação Python e PySpark no ambiente em nuvem do Azure Databricks. Para a normalização e modelagem de dados, foi utilizado SQL na plataforma dbt Cloud.

A arquitetura do projeto envolve recursos da Azure integrados ao dbt Cloud. Usando o Databricks como plataforma do processo de ELT, os dados extraídos da web são armazenados em um diretório inbound dentro de um contêiner da conta de armazenamento da Azure com a data de extração. Utilizamos o BeautifulSoup para identificar elementos e carregar informações na stage bronze. Com PySpark, carregamos todos os dados da stage bronze e passamos por uma limpeza e transformação de dados até o carregamento dos dados tratados em uma stage silver. Por fim, os dados são processados e carregados em uma tabela externa que está particionada pela data de extração.

No dbt, é feita a conexão do catálogo do Databricks e são criadas views de normalização de dados e métricas para análise de dados.

Todo o processo ocorre no workflow do Databricks de forma agendada, com alertas enviados por e-mail em caso de tempo de processo ou falha.

Os scripts são versionados e separados por ambientes de desenvolvimento (dev) e produção (prd).

![arquitetura-projeto-nintendo](https://github.com/user-attachments/assets/7e06bcbe-da5e-42a4-a9d2-bf7abaf7a238)

## Objetivo:

1. **Captação e Armazenamento de Dados Brutos**:
    - **Objetivo**: Capturar dados brutos diários de sites de e-commerce e marketplaces para análise posterior.
    - **Benefício**: Permite a reprocessamento dos dados caso os sites mudem, preservando a integridade dos dados históricos.

2. **Processamento de Dados com BeautifulSoup**:
    - **Objetivo**: Extrair informações essenciais dos arquivos HTML, como links, títulos, preços, promoções, parcelamentos e imagens dos produtos.
    - **Benefício**: Flexibilidade para ajustar o processamento conforme necessário, sem a perda de dados brutos.

3. **Integração e Limpeza de Dados com PySpark**:
    - **Objetivo**: Unificar os dados extraídos em um único dataframe e realizar a limpeza e tratamento dos mesmos.
    - **Benefício**: Melhoria da qualidade dos dados, com correção de valores nulos e extração de informações adicionais dos títulos dos anúncios.

4. **Armazenamento Seguro dos Dados**:
    - **Objetivo**: Armazenar dados tratados em uma external table no Databricks, garantindo a segurança e integridade dos dados.
    - **Benefício**: Proteção dos dados em armazenamento externo, prevenindo perdas devido à exclusão de tabelas no Databricks.

5. **Normalização e Análise de Dados com DBT**:
    - **Objetivo**: Normalizar os dados em views e métricas para facilitar análises de dados.
    - **Benefício**: Separação de funções entre engenheiros de dados e analistas, aumentando a segurança e eficiência do processo.

6. **Automatização e Notificação**:
    - **Objetivo**: Automatizar todo o processo via workflows e notificações de falhas ou tempo limite.
    - **Benefício**: Aumenta a eficiência operacional e permite resposta rápida a problemas.

## Etapas do projeto:

### 1.Criação do storageaccount

- Com o grupo de recursos criado, o primeiro passo foi a criação de uma conta de armazenamento Gen2 com redundância local e camada cool, pois os dados serão acessados com pouca frequência por se tratar de um processo batch.
- Na mesma conta de armazenamento, foram criados dois containers, dev e prd, para separar os dados de produção daqueles em desenvolvimento.
- Em cada container, foi criada uma hierarquia de pastas para a construção do processo ELTL no modelo de medalhão, onde temos os seguintes dados:
    - Inbound: Dados brutos conforme vêm da extração web em formato HTML, separados pela paginação e data da extração.
    - Bronze: Identificação dos elementos web necessários para o projeto e armazenados em um arquivo JSON conforme a data de sua extração.
    - Silver: União de todos os arquivos em seus diferentes diretórios e o tratamento de limpeza e ajuste dos dados.
    - Gold: Criação da external table no databricks com a fonte de dados na conta de armazenamento particionada pela data de extração

### 2.Criação do Azure Databricks

Com o Azure Databricks criado sem nenhuma particularidade específica, basta acessar o workspace para realizar as configurações locais:
- Integrar o GitHub ao Databricks com um token de uso pessoal.
- Criação de um cluster: o Standard_DS3_v2 é mais que suficiente.
- Criação de schemas Dev e Prd no Catálogo do Databricks para separar os dados em ambientes.
- No espaço de trabalho, crie duas pastas, dev e prd, para separar os códigos em cada branch.

- Criação de uma credencial externa no workspace.
- Criação de dois external location para os container dev e prd (necessário para a criação da external table).

### 3.Craição de um Acess Conector

- Crie um Acess Conector com a mesma região e grupo de recurso do projeto.
- Atribua a função de Colaborador de Dados do Storage Blob ao acess conector.
- Criação de uma credencial externa no workspace do azure databricks.
- Criação de dois external location para os container dev e prd (necessário para a criação da external table e leituras e gravações de dados).
  
### 5.Conexão entre dbt e databricks

- Com a conta no dbt criada, crie seu projeto.
- Conecte o Databricks ao dbt utilizando as informações do cluster: o host, o caminho HTTP (http path) e a porta, que geralmente é a 443.
- Conecte o GitHub ao dbt com um token de uso pessoal.
- Com as conexões realizadas, crie dois arquivos YML para os schemas dev e prd, que serão as fontes de busca para trabalhar com os dados no Databricks.

### 6.Workflow Databricks

- Crie dois workflows: um com a tag hml, que se refere ao fluxo de teste, e outro com a tag prd, que será o fluxo de produção.
- Em cada workflow, habilite a integração com o Git para poder rodar processos do dbt.
- As tarefas que não forem relacionadas a processos do dbt serão realizadas com notebooks do espaço local, tornando dinâmica a tratativa entre os ambientes.
- Agende o workflow de sua preferência. Para cada ambiente, foi utilizada a sintaxe cron:
  - dev: 0 0 8 ? * MON-FRI *
  - prd: 0 0 9,13,17 ? * MON-FRI *
- Ative as notificações de falhas e tempo de processo para que você seja notificado por e-mail.
