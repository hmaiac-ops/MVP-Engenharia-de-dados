# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Busca e Coleta de Dados
# MAGIC
# MAGIC > **Sequência do projeto:** notebook 2 de 6. Pré-requisito: nenhum (este é o ponto de partida técnico do pipeline). Ao final deste notebook, as tabelas `main.bronze.telco_churn_raw`, `main.bronze.telco_location_raw` e `main.bronze.telco_population_raw` estarão disponíveis para o notebook `03_Silver`.
# MAGIC
# MAGIC ## Origem e Licença
# MAGIC Para atender ao objetivo deste projeto, foi selecionado o *dataset* **IBM Telco Customer Churn**, um conjunto de dados amplamente reconhecido no ecossistema de dados, enriquecido neste sprint com bases complementares de localização e densidade populacional. 
# MAGIC * **Fonte:** Os arquivos originais são disponibilizados pela comunidade em repositórios abertos (como Kaggle e GitHub) no formato estático estruturado (CSV). Para este pipeline, os três arquivos foram centralizados em um repositório público no GitHub.
# MAGIC * **Licença de Uso:** Os dados foram criados e disponibilizados pela IBM com fins educacionais e de demonstração. Por se tratar de uma base com informações anonimizadas e fictícias para uso acadêmico, não há restrições de confidencialidade (LGPD) ou licenças impeditivas comerciais.
# MAGIC
# MAGIC ## Estratégia de Ingestão (Camada Bronze)
# MAGIC Em um cenário corporativo, a coleta poderia ocorrer via extração de um banco relacional ou consumo de uma API. Neste MVP, simularemos essa ingestão automatizada consumindo os arquivos originais diretamente de um repositório público na web e persistindo no armazenamento em nuvem do Databricks no formato Delta Lake. 
# MAGIC
# MAGIC O dado será gravado em seu estado bruto, inaugurando a camada **Bronze** da nossa arquitetura Medalhão. Contudo, devido a uma restrição estrutural do mecanismo de armazenamento em parquet/Delta do Spark, que não suporta espaços ou caracteres especiais em nomes de atributos, foi necessário aplicar um saneamento de *schema* imediato durante a leitura. Colunas com espaçamentos foram padronizadas já com seus nomes finais nesta etapa (convertendo `Zip Code` para `Zip_Code`, `Lat Long` para `Lat_Long` e `Customer ID` para `customerID`, este último já no padrão `camelCase` usado nativamente pela tabela de *churn*), viabilizando a gravação das tabelas brutas sem erros estruturais e garantindo que o identificador do cliente já nasça consistente entre todas as fontes, sem precisar de nenhum ajuste adicional nas camadas seguintes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentação da Ingestão (Camada Bronze)
# MAGIC
# MAGIC Durante a etapa de ingestão de dados, o plano original era realizar o download dos arquivos `.csv` e armazená-los diretamente no sistema de arquivos padrão do Databricks (DBFS). No entanto, me deparei com um erro de permissão (`DBFS_DISABLED`). Isso ocorreu porque as configurações de segurança mais recentes do Unity Catalog restringem esse tipo de acesso na versão que estou utilizando.
# MAGIC
# MAGIC Para contornar essa limitação e garantir a ingestão eficiente dos dados para o ambiente de nuvem, desenvolvi um fluxo totalmente automatizado via código Python, estruturado nos seguintes passos:
# MAGIC
# MAGIC 1. **Leitura das Fontes:** Utilizei a biblioteca `pandas` para ler os arquivos diretamente do repositório no GitHub, carregando os dados originais temporariamente em memória.
# MAGIC 2. **Conversão para Spark:** Para aproveitar o processamento distribuído do Databricks nas etapas seguintes, converti esses DataFrames do Pandas para DataFrames do ecossistema Spark.
# MAGIC 3. **Saneamento Inicial de Schema:** Ao tentar gravar os dados, identifiquei uma restrição técnica própria do formato Delta Lake (`[DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES]`), que bloqueia o uso de espaços em nomes de colunas. Para evitar falhas no pipeline, apliquei um tratamento imediato logo após a leitura, já normalizando cada coluna problemática direto para seu nome final: `Zip Code` para `Zip_Code`, `Lat Long` para `Lat_Long` e `Customer ID` para `customerID` (este último no mesmo padrão `camelCase` que a tabela de *churn* já usa nativamente, evitando qualquer divergência de nomenclatura entre as fontes desde o início do pipeline).
# MAGIC 4. **Persistência na Camada Bronze:** Como alternativa ao armazenamento de arquivos soltos em diretórios, criei um *database* específico chamado `bronze`. Nele, salvei os dados já com os *schemas* normalizados como tabelas nativas no formato Delta (ex: `main.bronze.telco_churn_raw`, `telco_location_raw` e `telco_population_raw`), alinhando o projeto às melhores práticas da arquitetura *Lakehouse*.
# MAGIC 5. **Validação de Ingestão:** Como última etapa, executei consultas exploratórias (`SELECT`) para confirmar a volumetria dos registros e visualizar as primeiras linhas, atestando que o carregamento das colunas ocorreu corretamente e sem perda de informações.

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col, regexp_replace

# 1. URLs dos arquivos raw no repositório do GitHub
url_churn = "https://raw.githubusercontent.com/hmaiac-ops/MVP-Engenharia-de-dados/refs/heads/main/WA_Fn-UseC_-Telco-Customer-Churn.csv"
url_population = "https://raw.githubusercontent.com/hmaiac-ops/MVP-Engenharia-de-dados/refs/heads/main/Telco_customer_churn_population.csv"
url_location = "https://raw.githubusercontent.com/hmaiac-ops/MVP-Engenharia-de-dados/refs/heads/main/Telco_customer_churn_location.csv"

print("Iniciando a ingestão dos dados do GitHub...")

# Lendo os dados brutos via Pandas
df_churn_pd = pd.read_csv(url_churn)
df_population_pd = pd.read_csv(url_population)
df_location_pd = pd.read_csv(url_location)

# Convertendo para Spark DataFrames
df_spark_churn = spark.createDataFrame(df_churn_pd)
df_spark_population = spark.createDataFrame(df_population_pd)
df_spark_location = spark.createDataFrame(df_location_pd)

# CORREÇÃO: Renomeando colunas com espaços para evitar o erro do Delta Lake
df_spark_population = df_spark_population.withColumnRenamed("Zip Code", "Zip_Code")
df_spark_location = df_spark_location.withColumnRenamed("Zip Code", "Zip_Code") \
                                      .withColumnRenamed("Customer ID", "customerID") \
                                      .withColumnRenamed("Lat Long", "Lat_Long")

# Criando o catálogo e o banco Bronze
spark.sql("CREATE CATALOG IF NOT EXISTS main;")
spark.sql("CREATE DATABASE IF NOT EXISTS main.bronze;")

# Salvando as tabelas brutas no formato Delta Lake
df_spark_churn.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.bronze.telco_churn_raw")
df_spark_population.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.bronze.telco_population_raw")
df_spark_location.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.bronze.telco_location_raw")

print("Dados ingeridos, colunas renomeadas e salvos com sucesso na camada Bronze!")

for tabela in ["telco_churn_raw", "telco_location_raw", "telco_population_raw"]:
    df = spark.read.table(f"main.bronze.{tabela}")
    print(f"{tabela}: {df.count()} linhas")
    display(df.limit(5))