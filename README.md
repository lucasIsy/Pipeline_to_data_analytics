<img width="1340" height="496" alt="Image" src="https://github.com/user-attachments/assets/267e6d06-1463-43a3-a1c6-8c1dffd67402" />

# Raíz do problema e Solução
Era necessário extrair metricas de 4 jogos para um projeto de análise de dados, porém, a extração dos dados via webscrapping(SteamDB e TwitchTracker) era impossibilitada pelo bloqueio da cloudflare. Esse projeto soluciona esse problema criando um pipeline que tem como fonte de dados as APIs oficiais(Steam/Twitch), garantindo escalabilidade e otimização dos recursos.

*Esse projeto não tem relação oficial com a Steam ou Twitch, e os dados extraídos vão de acordo com as normas estabelecidas por cada uma.*

## Estrutura do pipeline

### Orquestração
Os dados são extraídos via REQUEST(HTTP) com autenticação Oauth a cada 10 minutos para o bucket do google storage. Depois, a cada 24 horas o Databricks Jobs executa o processamento incremental e atualiza todas as camadas(bronze, silver e gold).
- Airflow
- Docker
- Databricks Jobs
- REQUEST(HTTP)
- Oauth
  
### Armazenamento
Pelo fato do monitoramento ocorrer de 10 em 10 minutos, em um unico mes é gerado 8.928 arquivos jsons. Para reduzir os custos com armazenamento, os dados no GCP storage persistem por 7 dias, ao mesmo tempo que os arquivos JSONs vão para uma tabela cumulativa do tipo delta no qual reduz em 83% o custo com armazenamento. [duas estrategias persistencia e delta]
- GCP Storage
- Delta Lake
  
### Arquitetura Medalhão
- Bronze: 
	- compacta os arquivos json de forma incremental em uma tabela delta.
- Staging: 
	- Padroniza os nomes de colunas e aplica testes de integridade do pipeline
- Silver:
	- deduplicação dos dados causados pela API da Twitch
	- criação de tabelas acumulativas com as métricas históricas dos jogos
- Gold
	- Tabela fato com dados agregas por mês otimizada para analise de dados e BI
	- Tabela gold para twitch e steam para usos separados.
   
### Modelagem dos dados
- **Camada Silver:** Os dados são deduplicados e normalizados, garantindo que cada evento da API seja único e tipado corretamente. Os dados vão para uma tabela cumulativa no qual armazena o histórico das métricas em um array de forma incremental. Esse tipo de tabela foi escolhido para garantir que, independente da quantidade de jogos, os JOINS não aumentem drasticamente os custos com processamento.
- **Camada Gold:** Os dados são modelados em tabelas de fatos e agregados mensais (ex: `fact_twitch_monthly`). Isso reduz o custo de processamento para os dashboards e simplifica a criação de KPIs de negócio.

### Teste de integridade e qualidade
Implementei **Data Contracts** em todas as camadas do pipeline para manter a integridade e facilitar a manutenção em casos de erros.
- **Bronze:** Validação de Schema JSON na ingestão para integridade da fonte.
- **Silver:** Testes dbt `not_null, unique, column_value_type` eliminando duplicatas e garantindo que os dados cheguem da forma esperada.
- **Gold:** Testes de consistência de negócio para assegurar métricas acuradas.
  
### Governança dos dados
- **Controle de Acesso (GCP IAM):** Apliquei o princípio do "menor privilégio" no **Google Cloud Storage**, configurando _Service Accounts_ específicas para que o Airflow gerencie a Landing Zone sem expor dados sensíveis.
    
- **Catálogo e Linhagem (Unity Catalog):** Utilizei o **Unity Catalog** no Databricks para centralizar o dicionário de dados e garantir a _Data Lineage_ (linhagem), permitindo rastrear a origem de cada métrica desde a Bronze até a Gold.
    
- **Gestão de Ciclo de Vida:** Configurei políticas de retenção de 7 dias na camada de pouso (Landing Zone) para otimizar custos e garantir a conformidade com boas práticas de segurança
