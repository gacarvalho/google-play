# 🧭 ♨️ COMPASS - Ingestão Google Play

<p align="left">
  <img src="https://img.shields.io/badge/projeto-Compass-blue?style=flat-square" alt="Projeto">
  <img src="https://img.shields.io/badge/versão-1.0.0-blue?style=flat-square" alt="Versão">
  <img src="https://img.shields.io/badge/status-deployed-green?style=flat-square" alt="Status">
  <img src="https://img.shields.io/badge/autor-Gabriel_Carvalho-lightgrey?style=flat-square" alt="Autor">
</p>

Essa aplicação faz parte do projeto **compass-deployment**, uma solução desenvolvida no contexto do programa **Data Master**, promovido pela F1rst Tecnologia. O objetivo é prover uma plataforma escalável para captura, processamento e análise de **feedbacks de usuários** dos aplicativos do Banco Santander, com foco na **Google Play Store**.

![<data-master-compass>](https://github.com/gacarvalho/repo-spark-delta-iceberg/blob/main/header.png?raw=true)

---

## 📦 Artefato

- **Imagem Docker:** `iamgacarvalho/dmc-app-ingestion-reviews-google-play-hdfs-compass:1.0.1`
- **Repositório GitHub:** [https://github.com/gacarvalho/google-play](https://github.com/gacarvalho/google-play)
- **Descrição:** Coleta avaliações de usuários através da **Google Play Store**, valida os dados e armazena no **HDFS** em formato **Parquet**.

---

## ⚙️ Parâmetros de Execução

| Parâmetro     | Descrição                                                                 |
|---------------|---------------------------------------------------------------------------|
| `$CONFIG_ENV` | Define o ambiente de execução: `Pre` (Pré-Produção), `Pro` (Produção)     |
| `$PARAM1`     | ID do app na Google Play (ex: `com.santander.app`)                        |
| `$PARAM2`     | Nome do canal/app (usar hífen `-` como separador se necessário)           |
| `$PARAM3`     | Segmento do cliente: `pf` (Pessoa Física), `pj` (Pessoa Jurídica)         |

---

## 📊 Visão Geral

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

---

## 📝 Exemplo de Execução

```bash
spark-submit repo_extc_google_play.py Pre com.santander.app santander-way pf
