SHELL := /bin/bash

REGISTRY=171649450587.dkr.ecr.eu-west-1.amazonaws.com
PROXY_URL=http://emea-aws-webproxy.service.cloud.local:3128
NO_PROXY=172.20.0.1/16,10.24.224.0/23,10.24.184.0/21,::1,localhost,127.0.0.1,169.254.169.254

PLATFORM_ARGS= \
	--build-arg HTTP_PROXY=${PROXY_URL} \
	--build-arg HTTPS_PROXY=${PROXY_URL} \
	--build-arg NO_PROXY=${NO_PROXY} \
	--build-arg http_proxy=${PROXY_URL} \
	--build-arg https_proxy=${PROXY_URL} \
	--build-arg no_proxy=${NO_PROXY} \
	--build-arg REPO=${REGISTRY}/

DOCKERFILE=./Dockerfile
TAG=latest


ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

BUILD_DIR=./

DEPLOYMENT_DIR=./conf/kubernetes/deployment/

CONFIG_FILE=./conf/settings.yaml