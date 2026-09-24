# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Camada Silver — Limpeza e Padronização
# MAGIC
# MAGIC > **Sequência do projeto:** notebook 3 de 6. Pré-requisito: execute `02_Coleta_e_Bronze` antes deste. Ao final, as tabelas `main.silver.telco_churn_cleaned`, `main.silver.telco_location_cleaned` e `main.silver.telco_population_cleaned` estarão disponíveis para o notebook `04_Gold`.
# MAGIC
# MAGIC ## Limpeza e Tratamento (Camada Silver)
# MAGIC Durante a análise de qualidade dos dados brutos, esbarrei em alguns desafios típicos de engenharia de dados. Na tabela principal de *churn*, a coluna `TotalCharges` (Faturamento Total) veio importada como texto (*string*), e os clientes muito novos (com *tenure* igual a zero) traziam espaços em branco em vez de valores numéricos. Para contornar isso, apliquei um tratamento na camada **Silver** para converter esses vazios em `"0.0"` e transformar a coluna em tipo numérico (`float`).
# MAGIC
# MAGIC Como o projeto integra desde o início três fontes distintas, também foi necessário aplicar saneamentos nas bases complementares. Na tabela de população, limpei formatações de texto e converti os dados para inteiros. A padronização de nomes de colunas com espaço (como `Zip Code` e `Customer ID`) já havia sido aplicada por completo na camada Bronze, inclusive a padronização final do identificador do cliente para `customerID` (minúsculo, sem underscore), que já chega pronto desde a ingestão. Por isso, a camada Silver não precisa fazer nenhum ajuste de nomenclatura na tabela de localização, só a limpeza numérica da população.

# COMMAND ----------

from pyspark.sql.functions import col, when, trim, regexp_replace

# 1. Leitura dos dados brutos da camada Bronze
df_bronze_churn = spark.read.table("main.bronze.telco_churn_raw")
df_bronze_location = spark.read.table("main.bronze.telco_location_raw")
df_bronze_population = spark.read.table("main.bronze.telco_population_raw")

# ==========================================
# CAMADA SILVER: Limpeza, Saneamento e Padronização
# ==========================================
spark.sql("CREATE DATABASE IF NOT EXISTS main.silver;")

# A. Churn: Tratamento de TotalCharges e padronização do ID para 'customerID'
df_silver_churn = df_bronze_churn.withColumn(
    "TotalCharges",
    when(trim(col("TotalCharges")) == "", "0.0").otherwise(col("TotalCharges"))
).withColumn("TotalCharges", col("TotalCharges").cast("float")) \
 .withColumnRenamed("Customer ID", "customerID") # Caso venha com espaço na origem

df_silver_churn.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.silver.telco_churn_cleaned")

# B. Location: os saneamentos de nome de coluna (Customer ID -> customerID,
# Zip Code -> Zip_Code, Lat Long -> Lat_Long) já foram todos aplicados na
# camada Bronze; aqui a tabela é apenas persistida como Silver, sem
# transformação adicional de schema pendente.
df_silver_loc = df_bronze_location

df_silver_loc.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.silver.telco_location_cleaned")

# C. Population: 'Zip Code' -> 'Zip_Code' já foi normalizado na camada Bronze;
# aqui aplicamos o único tratamento real desta tabela: limpeza da formatação
# numérica (remoção da vírgula de milhar) e conversão para inteiro
df_silver_pop = df_bronze_population \
    .withColumn(
        "Population", 
        regexp_replace(col("Population"), ",", "").cast("integer")
    )

df_silver_pop.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.silver.telco_population_cleaned")

print("Camada Silver processada com sucesso!")