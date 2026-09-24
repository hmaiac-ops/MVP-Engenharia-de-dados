# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Análise de Dados
# MAGIC
# MAGIC > **Sequência do projeto:** notebook 5 de 6. Pré-requisito: execute `04_Gold` antes deste (e `02_Coleta_e_Bronze`, já que a checagem de qualidade lê direto da camada Bronze).
# MAGIC
# MAGIC ## a. Análise de Qualidade de Dados
# MAGIC
# MAGIC Para não me basear só em impressão, rodei uma checagem de qualidade em cada atributo das três tabelas ainda na camada Bronze, antes de qualquer limpeza, cobrindo nulos/vazios, domínio de valores, faixa numérica e duplicidade de chave primária.
# MAGIC
# MAGIC **Valores nulos e vazios:** das 21 colunas da base principal de churn, só uma apresentou problema: TotalCharges teve 11 registros vazios, todos referentes a clientes muito recentes (tenure = 0), confirmando o motivo do tratamento que já apliquei na camada Silver. As tabelas de localização e população vieram completamente limpas, sem nenhum nulo ou vazio em nenhuma coluna.
# MAGIC
# MAGIC **Domínio das colunas categóricas:** verifiquei a distribuição de valores de todas as colunas categóricas (gender, Contract, InternetService, PaymentMethod, Churn, entre outras) e todas vieram exatamente dentro dos domínios que já havia documentado no Catálogo de Dados, sem nenhuma categoria inesperada ou mal formatada.
# MAGIC
# MAGIC **Faixa de valores numéricos:** tenure variou de 0 a 72 meses e MonthlyCharges de 18,25 a 118,75, dados coerentes com o esperado para uma base de assinaturas mensais. Já o Population, na tabela bruta, veio como texto com vírgula de milhar (ex: "54,492"), o que impedia qualquer comparação numérica correta. Ao tentar calcular o mínimo e o máximo direto da string, obtive um resultado sem sentido (mínimo maior que o máximo), evidenciando o problema. Depois de remover a formatação e converter para número, a faixa real ficou entre 11 e 105.285 habitantes por CEP, o que confirma que o tratamento aplicado na Silver (regexp_replace + conversão para inteiro) era realmente necessário. As coordenadas de Latitude e Longitude também ficaram dentro do intervalo geográfico esperado para o estado da Califórnia, o que serve como uma validação extra de que os dados de localização não têm distorção.
# MAGIC
# MAGIC **Duplicidade de chave primária:** por fim, verifiquei se customerID (na base de churn) e Zip_Code (na base de população) realmente funcionam como chaves únicas, sem repetição. O resultado confirmou isso: os 7.043 registros de customerID são todos distintos entre si, e o mesmo vale para os 1.671 registros de Zip_Code. Essa validação é importante porque garante que os joins entre as três fontes, na camada Gold, não vão duplicar linhas por engano.
# MAGIC
# MAGIC No geral, os problemas de qualidade encontrados eram pontuais e já haviam sido endereçados corretamente na camada Silver; essa checagem serviu para confirmar, com números, que o tratamento aplicado foi o adequado.

# COMMAND ----------

from pyspark.sql.functions import col, count, when, trim, regexp_replace, min as spark_min, max as spark_max, mean as spark_mean

df_bronze_churn = spark.read.table("main.bronze.telco_churn_raw")
df_bronze_location = spark.read.table("main.bronze.telco_location_raw")
df_bronze_population = spark.read.table("main.bronze.telco_population_raw")

# 1. NULOS E VAZIOS POR COLUNA (cobre strings vazias, não só NULL de verdade)
def checar_nulos_e_vazios(df, nome_tabela):
    print(f"--- Nulos/vazios por coluna: {nome_tabela} ---")
    exprs = []
    for c, tipo in df.dtypes:
        if tipo == "string":
            exprs.append(count(when(col(c).isNull() | (trim(col(c)) == ""), c)).alias(c))
        else:
            exprs.append(count(when(col(c).isNull(), c)).alias(c))
    display(df.select(exprs))

checar_nulos_e_vazios(df_bronze_churn, "telco_churn_raw")
checar_nulos_e_vazios(df_bronze_location, "telco_location_raw")
checar_nulos_e_vazios(df_bronze_population, "telco_population_raw")

# 2. DOMÍNIO DAS COLUNAS CATEGÓRICAS (uma tabela de frequência por coluna)
colunas_categoricas = ["gender", "SeniorCitizen", "Partner", "Dependents", "PhoneService",
                        "MultipleLines", "InternetService", "OnlineSecurity", "OnlineBackup",
                        "DeviceProtection", "TechSupport", "StreamingTV", "StreamingMovies",
                        "Contract", "PaperlessBilling", "PaymentMethod", "Churn"]

for c in colunas_categoricas:
    print(f"--- Domínio de valores: {c} ---")
    display(df_bronze_churn.groupBy(c).count().orderBy("count", ascending=False))

# 3. FAIXA DE VALORES DAS COLUNAS NUMÉRICAS (min/max/média)
print("--- Estatísticas: tenure, MonthlyCharges ---")
display(df_bronze_churn.select("tenure", "MonthlyCharges").describe())

print("--- Estatísticas: Population (bruto, como string) ---")
display(df_bronze_population.select(spark_min("Population"), spark_max("Population")))

print("--- Estatísticas: Population (após remover formatação e converter) ---")
df_pop_numerico = df_bronze_population.withColumn(
    "Population_num", regexp_replace(col("Population"), ",", "").cast("integer")
)
display(df_pop_numerico.select(spark_min("Population_num"), spark_max("Population_num")))

print("--- Estatísticas: Latitude, Longitude ---")
display(df_bronze_location.select("Latitude", "Longitude").describe())

# 4. DUPLICIDADE DE CHAVE PRIMÁRIA (checagem essencial antes de modelar)
total_churn = df_bronze_churn.count()
distintos_churn = df_bronze_churn.select("customerID").distinct().count()
print(f"telco_churn_raw -> total: {total_churn} | customerID distintos: {distintos_churn}")

total_pop = df_bronze_population.count()
distintos_pop = df_bronze_population.select("Zip_Code").distinct().count()
print(f"telco_population_raw -> total: {total_pop} | Zip Code distintos: {distintos_pop}")

# COMMAND ----------

# Vamos usar o Spark SQL para responder às perguntas analíticas cruzando todas as tabelas da camada Gold (incluindo dimensões, serviços, contratos e localização/população)

print("--- PERGUNTA 1: Impacto do contrato no Churn ---")
display(spark.sql("""
    SELECT 
        c.Contract, 
        COUNT(*) as total_clientes,
        SUM(CASE WHEN f.Churn = 'Yes' then 1 else 0 end) as total_churn,
        ROUND(SUM(CASE WHEN f.Churn = 'Yes' then 1 else 0 end) * 100.0 / COUNT(*), 2) as taxa_churn_percentual
    FROM main.gold.fato_faturamento f
    JOIN main.gold.dim_contrato c ON f.id_contrato = c.id_contrato
    GROUP BY c.Contract
    ORDER BY taxa_churn_percentual DESC
"""))

print("--- PERGUNTA 2: Faturamento médio por tecnologia de internet ---")
display(spark.sql("""
    SELECT 
        s.InternetService, 
        ROUND(AVG(f.TotalCharges), 2) as faturamento_medio_total,
        ROUND(AVG(f.MonthlyCharges), 2) as faturamento_medio_mensal
    FROM main.gold.fato_faturamento f
    JOIN main.gold.dim_servicos s ON f.id_servico = s.id_servico
    GROUP BY s.InternetService
    ORDER BY faturamento_medio_total DESC
"""))

print("--- PERGUNTA 3: Impacto do Suporte Técnico no Churn ---")
display(spark.sql("""
    SELECT 
        s.TechSupport, 
        COUNT(*) as total_clientes,
        ROUND(SUM(CASE WHEN f.Churn = 'Yes' then 1 else 0 end) * 100.0 / COUNT(*), 2) as taxa_churn_percentual
    FROM main.gold.fato_faturamento f
    JOIN main.gold.dim_servicos s ON f.id_servico = s.id_servico
    GROUP BY s.TechSupport
"""))

print("--- PERGUNTA 4: Forma de pagamento dos clientes mais longevos e rentáveis ---")
display(spark.sql("""
    SELECT 
        c.PaymentMethod, 
        ROUND(AVG(f.tenure), 1) as media_meses_permanencia,
        ROUND(AVG(f.TotalCharges), 2) as media_faturamento_total
    FROM main.gold.fato_faturamento f
    JOIN main.gold.dim_contrato c ON f.id_contrato = c.id_contrato
    GROUP BY c.PaymentMethod
    ORDER BY media_faturamento_total DESC
"""))

print("--- PERGUNTA 5: Taxa de Churn por Cidade ---")
display(spark.sql("""
    SELECT 
        l.City, 
        COUNT(*) as total_clientes,
        SUM(CASE WHEN f.Churn = 'Yes' then 1 else 0 end) as total_churn,
        ROUND(SUM(CASE WHEN f.Churn = 'Yes' then 1 else 0 end) * 100.0 / COUNT(*), 2) as taxa_churn_percentual
    FROM main.gold.fato_faturamento f
    JOIN main.gold.dim_localizacao l ON f.customerID = l.customerID
    GROUP BY l.City
    HAVING COUNT(*) >= 30
    ORDER BY taxa_churn_percentual DESC
"""))

print("--- PERGUNTA 6: Faturamento médio e Churn cruzando com dados Populacionais ---")
display(spark.sql("""
    SELECT 
        CASE 
            WHEN l.Population < 10000 THEN 'Baixa Densidade (< 10k)'
            WHEN l.Population BETWEEN 10000 AND 30000 THEN 'Média Densidade (10k-30k)'
            ELSE 'Alta Densidade (> 30k)'
        END as faixa_populacional,
        COUNT(*) as total_clientes,
        ROUND(AVG(f.MonthlyCharges), 2) as faturamento_medio_mensal,
        ROUND(SUM(CASE WHEN f.Churn = 'Yes' then 1 else 0 end) * 100.0 / COUNT(*), 2) as taxa_churn_percentual
    FROM main.gold.fato_faturamento f
    JOIN main.gold.dim_localizacao l ON f.customerID = l.customerID
    GROUP BY faixa_populacional
    ORDER BY faturamento_medio_mensal DESC
"""))

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

# --- Gráfico 1: Taxa de churn por tipo de contrato (Pergunta 1) ---
df_contrato = spark.sql("""
    SELECT 
        c.Contract, 
        ROUND(SUM(CASE WHEN f.Churn = 'Yes' then 1 else 0 end) * 100.0 / COUNT(*), 2) as taxa_churn_percentual
    FROM main.gold.fato_faturamento f
    JOIN main.gold.dim_contrato c ON f.id_contrato = c.id_contrato
    GROUP BY c.Contract
    ORDER BY taxa_churn_percentual DESC
""").toPandas()

plt.figure(figsize=(7, 4))
plt.bar(df_contrato["Contract"], df_contrato["taxa_churn_percentual"], color="#D85A30")
plt.title("Taxa de churn por tipo de contrato")
plt.ylabel("Taxa de churn (%)")
plt.xlabel("")
for i, v in enumerate(df_contrato["taxa_churn_percentual"]):
    plt.text(i, v + 1, f"{v}%", ha="center")
plt.tight_layout()
plt.show()

# --- Gráfico 2: Taxa de churn por cidade, só cidades com 30+ clientes (Pergunta 5) ---
df_cidade = spark.sql("""
    SELECT 
        l.City, 
        COUNT(*) as total_clientes,
        ROUND(SUM(CASE WHEN f.Churn = 'Yes' then 1 else 0 end) * 100.0 / COUNT(*), 2) as taxa_churn_percentual
    FROM main.gold.fato_faturamento f
    JOIN main.gold.dim_localizacao l ON f.customerID = l.customerID
    GROUP BY l.City
    HAVING COUNT(*) >= 30
    ORDER BY taxa_churn_percentual DESC
    LIMIT 10
""").toPandas()

plt.figure(figsize=(8, 4))
plt.barh(df_cidade["City"], df_cidade["taxa_churn_percentual"], color="#0F6E56")
plt.title("Top 10 cidades com maior taxa de churn (mín. 30 clientes)")
plt.xlabel("Taxa de churn (%)")
plt.gca().invert_yaxis()  # maior taxa no topo
plt.tight_layout()
plt.show()

# --- Gráfico 3: Faturamento médio e churn por faixa de densidade populacional (Pergunta 6) ---
df_densidade = spark.sql("""
    SELECT 
        CASE 
            WHEN l.Population < 10000 THEN 'Baixa Densidade (< 10k)'
            WHEN l.Population BETWEEN 10000 AND 30000 THEN 'Média Densidade (10k-30k)'
            ELSE 'Alta Densidade (> 30k)'
        END as faixa_populacional,
        COUNT(*) as total_clientes,
        ROUND(AVG(f.MonthlyCharges), 2) as faturamento_medio_mensal,
        ROUND(SUM(CASE WHEN f.Churn = 'Yes' then 1 else 0 end) * 100.0 / COUNT(*), 2) as taxa_churn_percentual
    FROM main.gold.fato_faturamento f
    JOIN main.gold.dim_localizacao l ON f.customerID = l.customerID
    GROUP BY faixa_populacional
""").toPandas()

# Reordenando as faixas na ordem lógica (baixa -> média -> alta), 
# já que a query original ordena por faturamento, não por densidade
ordem = ["Baixa Densidade (< 10k)", "Média Densidade (10k-30k)", "Alta Densidade (> 30k)"]
df_densidade["faixa_populacional"] = pd.Categorical(df_densidade["faixa_populacional"], categories=ordem, ordered=True)
df_densidade = df_densidade.sort_values("faixa_populacional")

fig, ax1 = plt.subplots(figsize=(8, 4.5))

# Barras: faturamento médio mensal (eixo esquerdo)
ax1.bar(df_densidade["faixa_populacional"], df_densidade["faturamento_medio_mensal"], color="#378ADD", label="Faturamento médio mensal")
ax1.set_ylabel("Faturamento médio mensal ($)", color="#185FA5")
ax1.tick_params(axis="y", labelcolor="#185FA5")

# Linha: taxa de churn (eixo direito)
ax2 = ax1.twinx()
ax2.plot(df_densidade["faixa_populacional"], df_densidade["taxa_churn_percentual"], color="#D85A30", marker="o", linewidth=2, label="Taxa de churn")
ax2.set_ylabel("Taxa de churn (%)", color="#993C1D")
ax2.tick_params(axis="y", labelcolor="#993C1D")

plt.title("Faturamento e churn por densidade populacional")
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## b. Solução do Problema e Discussão dos Resultados
# MAGIC
# MAGIC Após estruturar a camada Gold com o Esquema Estrela, integrando os dados transacionais de churn, pacotes de serviços, condições contratuais e as bases geográficas e demográficas por código postal, executei consultas SQL para responder às perguntas de negócio do projeto. O processo permitiu transformar os dados brutos em respostas analíticas concretas:
# MAGIC
# MAGIC * **O impacto do tipo de contrato no Churn:** Os números deixaram claro que o modelo de contratação é um fator crítico para a retenção. Clientes que fecham contratos mês a mês cancelam muito mais, com uma taxa de evasão expressiva. Em contrapartida, assinaturas de um ou dois anos reduzem drasticamente o cancelamento, indicando que a fidelização de longo prazo protege a receita da operadora.
# MAGIC * **Faturamento por tecnologia de internet:** O cruzamento mostrou que clientes que utilizam fibra ótica (*Fiber optic*) possuem faturamento mensal e total consideravelmente superior às demais tecnologias. Embora seja um serviço de alta rentabilidade, exige atenção redobrada no pós-venda devido à forte concorrência nesse segmento.
# MAGIC * **O papel do suporte técnico na retenção:** A análise de serviços adicionais revelou que clientes com suporte técnico possuem taxas de cancelamento visivelmente menores. Isso comprova que empacotar serviços extras ajuda a ancorar o cliente na base, reduzindo a propensão de migração para concorrentes.
# MAGIC * **Formas de pagamento e longevidade:** Cruzando as formas de pagamento com o tempo de casa e o faturamento total, notei que métodos automáticos (como cartão de crédito) estão associados a maior permanência e maior receita acumulada. A fricção do pagamento manual em cheques ou cheques eletrônicos parece facilitar o esquecimento ou a decisão de churn.
# MAGIC * **Variação de Churn por Cidade:** Ao integrar os dados da tabela de localização (dim_localizacao), inicialmente tentei olhar a variação de cancelamento por Estado, mas percebi que toda a base está concentrada na Califórnia, ou seja, não havia nenhuma variação real para analisar nesse nível. Refiz a análise agrupando por Cidade, que é o recorte que realmente faz sentido nessa base. Nessa nova visão, também notei que boa parte das cidades tem poucos clientes (às vezes só 2 ou 3), o que fazia aparecer taxas de 100% de cancelamento que não significam muita coisa de verdade (pode ser só coincidência do grupo pequeno). Para não tirar conclusão errada, filtrei o resultado para considerar só cidades com uma quantidade mínima de clientes (usei HAVING COUNT(*) >= 30). Com esse filtro, dá pra ver de forma bem mais confiável quais cidades realmente têm um padrão de cancelamento mais alto, e isso ajuda a pensar em campanhas de retenção direcionadas para essas regiões específicas.
# MAGIC * **Densidade populacional, receita e churn:** Ao cruzar os dados de localização com as faixas de densidade demográfica obtidas da base de população (Population), percebi um padrão interessante: o faturamento médio mensal fica praticamente estável entre as três faixas (em torno de $63 a $65), ou seja, densidade populacional não parece influenciar quanto o cliente paga. Já a taxa de churn sobe de forma visível conforme a densidade aumenta(de cerca de 24,5% em áreas de baixa densidade para quase 31% em áreas de alta densidade). Uma hipótese razoável é que regiões mais densas concentram mais operadoras concorrentes, facilitando a troca de fornecedor. Isso abre espaço para estratégias comerciais diferentes por porte de mercado: reter por relacionamento em áreas densas (onde a concorrência é o problema), e por upsell em áreas menos densas (onde a base já é mais fiel).
# MAGIC
# MAGIC **Síntese geral:** Juntando as seis respostas, um padrão comercial claro emerge: o cliente mais propenso a cancelar é aquele com contrato mês a mês, sem serviços adicionais como suporte técnico, pagando por cheque ou cheque eletrônico (formas de pagamento manuais), e localizado em regiões urbanas mais densas, provavelmente por ter mais opções de concorrentes por perto. Do lado oposto, o cliente mais "protegido" contra churn é o que tem contrato de longo prazo, serviços agregados, pagamento automático e está em uma região menos densa. Do ponto de vista comercial, isso sugere uma priorização clara para ações de retenção: oferecer migração de contrato mensal para anual com desconto, incentivar a adesão a pagamento automático (menor fricção), e empacotar suporte técnico como parte de ofertas de fidelização, com atenção redobrada a clientes de fibra ótica em áreas de alta densidade populacional, que combinam alto valor de faturamento com maior risco de cancelamento.