#!/bin/bash

# Função para imprimir logs com timestamp
log() {
  local message=$1
  echo "$(date +"%Y-%m-%d %H:%M:%S") [INFO] $message"
}

# Função para tratar erros
error_exit() {
  local message=$1
  echo "$(date +"%Y-%m-%d %H:%M:%S") [ERROR] $message" >&2
  exit 1
}

# Função para validar as variáveis necessárias
validate_params() {
  if [[ -z "$SPARK_HOME" ]]; then
    error_exit "Variável SPARK_HOME não definida!"
  fi

  if [[ -z "$CONFIG_ENV" ]]; then
    error_exit "O parâmetro CONFIG_ENV (pre ou prod) é obrigatório!"
  fi

  if [[ -z "$PARAM1" ]] || [[ -z "$PARAM2" ]] || [[ -z "$PARAM3" ]]; then
    error_exit "Os parâmetros PARAM1, PARAM2 e PARAM3 são obrigatórios!"
  fi
}

# Função para carregar configurações do arquivo YAML
load_yaml_config() {
  local config_file=$1

  if [[ ! -f "$config_file" ]]; then
    error_exit "Arquivo de configuração YAML '$config_file' não encontrado!"
  fi

  # Carrega as configurações do YAML usando `yq`
  executor_memory=$(yq e '.spark.executor_memory' "$config_file")
  driver_memory=$(yq e '.spark.driver_memory' "$config_file")
  executor_cores=$(yq e '.spark.spark_executor_cores' "$config_file")
  executor_instances=$(yq e '.spark.spark_executor_instances' "$config_file" // "1")
  parallelism=$(yq e '.spark.spark_default_parallelism' "$config_file")
  shuffle_partitions=$(yq e '.spark.spark_sql_shuffle_partitions' "$config_file")
  network_timeout=$(yq e '.spark.spark_network_timeout' "$config_file" // "600s")


  log "Configurações carregadas do YAML: $config_file"
}

# Função para montar e executar o Spark Submit
run_spark_submit() {
  if [ -f /app/.env ]; then
    export $(grep -v '^#' /app/.env | xargs)
  else
    echo "Arquivo .env não encontrado!"
    exit 1
  fi

  local spark_cmd="$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.executor.memory=$executor_memory \
    --conf spark.driver.memory=$driver_memory \
    --conf spark.executor.cores=$executor_cores \
    --conf spark.executor.instances=$executor_instances \
    --conf spark.default.parallelism=$parallelism \
    --conf spark.sql.shuffle.partitions=$shuffle_partitions \
    --conf spark.pyspark.python=/usr/bin/python3 \
    --conf spark.pyspark.driver.python=/usr/bin/python3 \
    --conf spark.metrics.conf=/usr/local/spark/conf/metrics.properties \
    --conf spark.ui.prometheus.enabled=true \
    --conf spark.executor.processTreeMetrics.enabled=true \
    --packages ch.cern.sparkmeasure:spark-measure_2.12:0.16 \
    --py-files /app/dependencies.zip,/app/metrics.py,/app/tools.py,/app/schema_google.py \
    --conf spark.executorEnv.ES_USER=$ES_USER \
    --conf spark.executorEnv.ES_PASS=$ES_PASS\
    --conf spark.executorEnv.SERPAPI_KEY=$SERPAPI_KEY \
    --conf spark.driverEnv.ES_USER=$ES_USER \
    --conf spark.driverEnv.ES_PASS=$ES_PASS \
    --conf spark.driverEnv.SERPAPI_KEY=$SERPAPI_KEY\
    --conf spark.yarn.appMasterEnv.ES_USER=$ES_USER \
    --conf spark.yarn.appMasterEnv.ES_PASS=$ES_PASS \
    --conf spark.yarn.appMasterEnv.SERPAPI_KEY=$SERPAPI_KEY\
    --name dmc_ingestion_reviews_google_play_$CONFIG_ENV-$PARAM2  \
    /app/repo_extc_google_play.py $CONFIG_ENV $PARAM1 $PARAM2 $PARAM3"

  # Exibe o comando para depuração
  log "Comando spark-submit que será executado: $spark_cmd"

  # Executa o Spark Submit e captura o código de retorno
  eval $spark_cmd
  local exit_code=$?

  if [[ $exit_code -ne 0 ]]; then
    error_exit "Falha ao executar o Spark Submit (código de saída: $exit_code)."
  else
    log "Spark Submit executado com sucesso!"
  fi
}

# Início do Script
log "************************************************************"
log "Iniciando Execução de Spark Submit"
log "************************************************************"

echo "parametros: $CONFIG_ENV $PARAM1 $PARAM2 $PARAM3"

# Define o arquivo de configuração com base no ambiente
if [[ "$CONFIG_ENV" == "pre" ]]; then
  CONFIG_FILE="/app/spark-pre.yaml"
elif [[ "$CONFIG_ENV" == "prod" ]]; then
  CONFIG_FILE="/app/spark-pro.yaml"
else
  echo "Ambiente inválido! Use 'pre' ou 'prod', param enviado: $CONFIG_ENV."
  exit 1
fi

# Valida as variáveis de ambiente e parâmetros
validate_params

# Carrega as configurações do YAML
load_yaml_config "$CONFIG_FILE"

# Executa o Spark Submit
run_spark_submit

log "************************************************************"
log "Finalizando Execução de Spark Submit"
log "************************************************************"
