# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Autoavaliação
# MAGIC
# MAGIC > **Sequência do projeto:** notebook 6 de 6, último da sequência.
# MAGIC
# MAGIC O desenvolvimento deste MVP proporcionou um aprendizado prático expressivo sobre a construção de pipelines de dados em nuvem, alinhando conceitos teóricos de engenharia de dados à realidade do setor de telecomunicações com múltiplas fontes de dados.
# MAGIC
# MAGIC * **Consolidação da Arquitetura Medalhão e Integração Multidatasource:** A estruturação sequencial das camadas (Bronze, Silver e Gold) no Databricks, integrada ao Unity Catalog, permitiu compreender na prática o ciclo de vida dos dados. A unificação de três fontes distintas (dados transacionais de *churn*, base geográfica de localização e dados demográficos populacionais) e a respectiva modelagem em Esquema Estrela mostraram-se eficientes para organizar as dimensões e a tabela fato, simplificando consultas analíticas complexas.
# MAGIC * **Desafios Técnicos e Resolução de Problemas:** Durante a implementação, enfrentei desafios importantes de saneamento e compatibilidade estrutural. Além do tratamento da coluna `TotalCharges` para remover espaços em branco de clientes recém-chegados, foi necessário harmonizar discrepâncias de nomenclatura entre as bases já na camada Bronze, padronizando colunas com espaço no nome (`Zip Code` → `Zip_Code`, `Lat Long` → `Lat_Long`) e, principalmente, alinhando o identificador do cliente (`Customer ID` → `customerID`) ao mesmo padrão que a tabela de *churn* já usava nativamente, garantindo uma chave única e consistente para os *joins* entre `fato_faturamento` e `dim_localizacao` sem precisar de nenhum ajuste adicional nas camadas seguintes. Além disso, limpei formatações numéricas na base populacional, garantindo a integridade exigida pelo Delta Lake. Também aprendi, na prática, a importância de validar o tamanho da amostra antes de tirar conclusões geográficas: a análise por Estado não trouxe nenhuma variação (base 100% concentrada na Califórnia), e ao migrar para Cidade percebi que resultados de grupos muito pequenos podem distorcer a leitura (com isso passei a aplicar filtros de volume mínimo).
# MAGIC * **Cumprimento dos Objetivos Analíticos:** Todas as perguntas de negócio formuladas foram respondidas com sucesso com base nas tabelas transformadas da camada Gold. Os resultados obtidos revelaram-se coerentes com os desafios reais de retenção e perfil de consumo da base.
# MAGIC * **Direcionamentos Futuros:** Como evolução natural deste projeto para um ambiente produtivo corporativo, poderiam ser adotadas a automação da orquestração do pipeline por meio de rotinas agendadas (*Databricks Workflows*) e a implementação de testes automatizados de qualidade de dados para monitorar eventuais anomalias de forma proativa.