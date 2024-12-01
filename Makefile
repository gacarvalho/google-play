DOCKER_NETWORK = hadoop-network
ENV_FILE = hadoop.env
VERSION_REPOSITORY_DOCKER = 0.0.3

current_branch := $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "default-branch")

# Realiza criacao da REDE DOCKER_NETWORK
create-network:
	docker network create hadoop_network

  # ################## 1.1 #####################
  # APP { ingestao GOOGLE PLAY }
  # ############################################

build-app-ingestion-reviews-google-play-hdfs-compass:
	docker build -t iamgacarvalho/dmc-app-ingestion-reviews-google-play-hdfs-compass:$(VERSION_REPOSITORY_DOCKER)  ./application/google-play
	docker push iamgacarvalho/dmc-app-ingestion-reviews-google-play-hdfs-compass:$(VERSION_REPOSITORY_DOCKER)

restart-docker:
	sudo systemctl restart docker

down-services:
	docker rm -f $(docker ps -a -q)

