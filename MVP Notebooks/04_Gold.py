# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Modelagem e Organização (Camada Gold)
# MAGIC
# MAGIC > **Sequência do projeto:** notebook 4 de 6. Pré-requisito: execute `03_Silver` antes deste. Ao final, as tabelas `main.gold.dim_cliente`, `dim_servicos`, `dim_contrato`, `dim_localizacao` e `fato_faturamento` estarão disponíveis para o notebook `05_Analise`.
# MAGIC
# MAGIC Com todas as bases tratadas e padronizadas, o passo seguinte foi estruturar o **Esquema Estrela** que desenhei no catálogo de dados. Utilizei o PySpark para separar as informações em tabelas dimensionais bem definidas:
# MAGIC * `dim_cliente`: Dados demográficos e cadastrais do assinante.
# MAGIC * `dim_servicos` e `dim_contrato`: Agrupamentos lógicos tratados e isolados através de chaves substitutas geradas por *hash* (`MD5`).
# MAGIC * `dim_localizacao`: Cruzamento entre os dados geográficos de endereço/coordenadas e os dados populacionais usando a chave unificada `Zip_Code`.
# MAGIC
# MAGIC Para fechar o processo, consolidei as chaves estrangeiras e as métricas principais na tabela `fato_faturamento`. Salvei todas essas tabelas finais em formato Delta dentro da camada **Gold**, deixando o ambiente organizado e totalmente pronto para alimentar as consultas analíticas e dashboards da área de negócios.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catálogo de Dados
# MAGIC
# MAGIC Abaixo, apresento o dicionário detalhado das tabelas e colunas que estruturam o modelo dimensional, especificando os tipos e domínios esperados para garantir a qualidade dos dados (*Data Quality*):
# MAGIC
# MAGIC ### Tabela: dim_cliente
# MAGIC
# MAGIC | Coluna | Tipo | Descrição / Domínio |
# MAGIC |---|---|---|
# MAGIC | `customerID` | String | Chave primária. Código único de identificação do cliente. |
# MAGIC | `gender` | String | Gênero do cliente (`Female`, `Male`). |
# MAGIC | `SeniorCitizen` | Inteiro | Indica se o cliente é idoso (`0` = não, `1` = sim). |
# MAGIC | `Partner` | String | Indica se o cliente possui cônjuge/parceiro (`Yes`, `No`). |
# MAGIC | `Dependents` | String | Indica se possui dependentes (`Yes`, `No`). |
# MAGIC
# MAGIC ### Tabela: dim_servicos
# MAGIC
# MAGIC | Coluna | Tipo | Descrição / Domínio |
# MAGIC |---|---|---|
# MAGIC | `id_servico` | String | Chave primária gerada no pipeline via *hash* dos serviços contratados. |
# MAGIC | `PhoneService` / `MultipleLines` | String | Telefonia e múltiplas linhas (`Yes`, `No`, `No phone service`). |
# MAGIC | `InternetService` | String | Tecnologia do link de dados (`DSL`, `Fiber optic`, `No`). |
# MAGIC | `OnlineSecurity` / `OnlineBackup` / `DeviceProtection` / `TechSupport` | String | Serviços adicionais de valor agregado (`Yes`, `No`, `No internet service`). |
# MAGIC
# MAGIC ### Tabela: dim_contrato
# MAGIC
# MAGIC | Coluna | Tipo | Descrição / Domínio |
# MAGIC |---|---|---|
# MAGIC | `id_contrato` | String | Chave primária gerada no pipeline via *hash* das condições contratuais. |
# MAGIC | `Contract` | String | Modelo de fidelidade (`Month-to-month`, `One year`, `Two year`). |
# MAGIC | `PaperlessBilling` | String | Adesão ao faturamento digital (`Yes`, `No`). |
# MAGIC | `PaymentMethod` | String | Forma de pagamento (ex.: `Electronic check`, `Credit card (automatic)`,`Mailed check`, `Bank trasnfer (automatic)`). |
# MAGIC
# MAGIC ### Tabela: dim_localizacao
# MAGIC
# MAGIC | Coluna | Tipo | Descrição / Domínio |
# MAGIC |---|---|---|
# MAGIC | `customerID` | String | Chave estrangeira que vincula a localização ao cliente. |
# MAGIC | `Country` / `State` / `City` | String | Informações geográficas de endereço. |
# MAGIC | `Zip_Code` | String | Código postal. Obs: campo saneado para substituir o espaço original do arquivo bruto `Zip Code` por `Zip_Code`, garantindo compatibilidade com as exigências do Delta Lake. |
# MAGIC | `Latitude` / `Longitude` | Float | Coordenadas geográficas da região. |
# MAGIC | `Population` | Inteiro | População do CEP correspondente (Mín: `11`, Máx: `105.285`). Obs: valores confirmados na análise de qualidade da camada Bronze, após conversão do campo original (string com vírgula de milhar) para tipo numérico. |
# MAGIC
# MAGIC ### Tabela: fato_faturamento
# MAGIC
# MAGIC | Coluna | Tipo | Descrição / Domínio |
# MAGIC |---|---|---|
# MAGIC | `customerID` | String | Chave estrangeira de cliente. |
# MAGIC | `id_servico` | String | Chave estrangeira de serviços. |
# MAGIC | `id_contrato` | String | Chave estrangeira de contrato. |
# MAGIC | `tenure` | Inteiro | Meses de permanência do cliente na base (Mín: `0`, Máx: `72`). |
# MAGIC | `MonthlyCharges` | Float | Faturamento recorrente mensal (Mín: `18.25`, Máx: `118.75`). |
# MAGIC | `TotalCharges` | Float | Faturamento total acumulado (Mín: `0.0`). Obs: na base bruta vinha como string com espaços vazios para clientes novos; foi tratado e convertido para float na camada Silver. |
# MAGIC | `Churn` | String | Indicador binário de cancelamento do serviço (`Yes`, `No`). |

# COMMAND ----------

from pyspark.sql.functions import md5, concat_ws

# Lendo a tabela Silver de churn, gravada pelo notebook anterior (03_Silver).
# Como cada notebook roda em sessão própria, não há variável em memória
# compartilhada entre eles -- por isso lemos de volta da tabela Delta.
df_silver_churn = spark.read.table("main.silver.telco_churn_cleaned")

# ==========================================
# CAMADA GOLD: Modelagem em Esquema Estrela
# ==========================================
spark.sql("CREATE DATABASE IF NOT EXISTS main.gold;")

# A. dim_cliente
dim_cliente = df_silver_churn.select("customerID", "gender", "SeniorCitizen", "Partner", "Dependents").distinct()
dim_cliente.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.gold.dim_cliente")

# B. dim_servicos
colunas_servicos = ["PhoneService", "MultipleLines", "InternetService", "OnlineSecurity", 
                    "OnlineBackup", "DeviceProtection", "TechSupport", "StreamingTV", "StreamingMovies"]

dim_servicos = df_silver_churn.select(*colunas_servicos).distinct() \
    .withColumn("id_servico", md5(concat_ws("||", *colunas_servicos)))
dim_servicos.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.gold.dim_servicos")

# C. dim_contrato
colunas_contrato = ["Contract", "PaperlessBilling", "PaymentMethod"]
dim_contrato = df_silver_churn.select(*colunas_contrato).distinct() \
    .withColumn("id_contrato", md5(concat_ws("||", *colunas_contrato)))
dim_contrato.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.gold.dim_contrato")

# D. Criando a Tabela dim_localizacao
# A coluna customerID já chega padronizada desde a camada Bronze, então
# não é necessário nenhum rename aqui.
df_loc_clean = spark.read.table("main.silver.telco_location_cleaned")

df_pop_clean = spark.read.table("main.silver.telco_population_cleaned")

dim_localizacao = df_loc_clean.join(df_pop_clean, on="Zip_Code", how="left") \
    .select("customerID", "Country", "State", "City", "Zip_Code", "Latitude", "Longitude", "Population")

dim_localizacao.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.gold.dim_localizacao")

# E. Criando a Tabela fato_faturamento
fato = df_silver_churn \
    .join(dim_servicos, on=colunas_servicos, how="left") \
    .join(dim_contrato, on=colunas_contrato, how="left") \

fato_faturamento = fato.select(
    "customerID", "id_servico", "id_contrato", 
    "tenure", "MonthlyCharges", "TotalCharges", "Churn"
)
fato_faturamento.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.gold.fato_faturamento")

print("Tabelas da Camada Gold criadas com sucesso!")