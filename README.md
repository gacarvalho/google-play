# 🧭 ♨️ COMPASS - Ingestão Google Play

<p align="left">
  <img src="https://img.shields.io/badge/projeto-Compass-blue?style=flat-square" alt="Projeto">
  <img src="https://img.shields.io/badge/versão-1.0.1-blue?style=flat-square" alt="Versão">
  <img src="https://img.shields.io/badge/status-deployed-green?style=flat-square" alt="Status">
  <img src="https://img.shields.io/badge/autor-Gabriel_Carvalho-lightgrey?style=flat-square" alt="Autor">
</p>

Essa aplicação faz parte do projeto **compass-deployment**, uma solução desenvolvida no contexto do programa **Data Master**, promovido pela F1rst Tecnologia. O objetivo é prover uma plataforma escalável para captura, processamento e análise de **feedbacks de usuários** dos aplicativos de aplictivos de Instituições, com foco na **Google Play Store**.

![<data-master-compass>](https://github.com/gacarvalho/repo-spark-delta-iceberg/blob/main/header.png?raw=true)

---


`📦 artefato`  `iamgacarvalho/dmc-app-ingestion-reviews-google-play-hdfs-compass:1.0.1`

- **Versão:** `1.0.1`
- **Repositório:** [GitHub](https://github.com/gacarvalho/google-play)
- **Imagem Docker:** [Docker Hub](https://hub.docker.com/repository/docker/iamgacarvalho/dmc-app-ingestion-reviews-google-play-hdfs-compass/tags/1.0.1/sha256-df992cb185f7a17ed0d40306e91d50139553e17e5c2a4d900579a0d42b804d9e)
- **Descrição:**  Coleta avaliações de clientes do Banco Santander armazenadas no  **Google Play**, processa os dados e os armazena no **HDFS** em formato **Parquet**.
- **Parâmetros:**

    - `$CONFIG_ENV` (`Pre`, `Pro`) → Define o ambiente: `Pre` (Pré-Produção), `Pro` (Produção).
    - `$PARAM1` (`name-review-localizado-na-url-do-app`). → Identificado (string) do aplicativo do Google Play, podendo ser localizado na URL na loja do Google Play, exemplo: `https://play.google.com/store/apps/details?id=br.com.santander.way&hl=pt_BR&pli=1`, nesse caso, o ID que vai no parametro é: `br.com.santander.way`.
    - `$PARAM2` (`nome-do-canal-ou-app`). → Nome do canal/app no Google Play. Para novos, use hífen (-).
    - `$PARAM3` (`pf`,`pj`). → Indicador do segmento do cliente. `PF` (Pessoa Física), `PJ` (Pessoa Juridica)


| Componente          | Descrição                                                                 |
|---------------------|---------------------------------------------------------------------------|
| **Objetivo**        | Coletar, processar e armazenar avaliações da Google Play Store            |
| **Entrada**         | ID do app, nome do app, tipo de cliente, ambiente (pre/prod)              |
| **Saída**           | Dados válidos/inválidos em Parquet + métricas no Elasticsearch            |
| **Tecnologias**     | PySpark, Elasticsearch, Parquet, SparkMeasure                             |
| **Fluxo Principal** | 1. Coleta API → 2. Validação → 3. Separação → 4. Armazenamento            |
| **Validações**      | Campos nulos, resposta do cliente, duplicações, tipos de dados            |
| **Particionamento** | Por data de carga (`odate`)                                               |
| **Métricas**        | Tempo de execução, registros válidos/inválidos, consumo Spark             |
| **Tratamento Erros**| Logs detalhados, salvamento separado dos registros inválidos             |
| **Execução**        | `spark-submit repo_extc_google_play.py <env> <product_id> <app> <cliente>` |
