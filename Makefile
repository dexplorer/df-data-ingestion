# Local

APP := ingest_app

install-dev: pyproject.toml
	pip install --upgrade pip &&\
	pip install --editable .[all-dev]

lint:
	pylint --disable=R,C src/${APP}/*.py &&\
	pylint --disable=R,C src/${APP}/*/*.py &&\
	pylint --disable=R,C src/*/*.py &&\
	pylint --disable=R,C tests/*.py

test:
	python -m pytest -vv --cov=src/${APP} tests

format:
	black src/${APP}/*.py &&\
	black src/${APP}/*/*.py &&\
	black src/*/*.py &&\
	black tests/*.py

local-all: install-dev lint format test

# Docker

IMAGE := df-data-ingestion
IMAGE_TAG := latest
PYTHON_VERSION := 3.12
# HADOOP_AWS_VERSION := 3.4.1
HADOOP_AWS_VERSION := 3.3.4
# AWS_JAVA_SDK_VERSION := 2.31.21
AWS_JAVA_SDK_VERSION := 1.12.782
HOST_PORT := 9090
CONTAINER_PORT := 9090

# export DOCKER_BUILDKIT := 1

install: pyproject.toml
	pip${PYTHON_VERSION} install --no-cache-dir .[all]

build-image:
	docker build \
	--build-context utils=/home/ec2-user/workspaces/utils \
	--build-context df-metadata=/home/ec2-user/workspaces/df-metadata \
	--build-context df-app-calendar=/home/ec2-user/workspaces/df-app-calendar \
	--build-context df-config=/home/ec2-user/workspaces/df-config \
	--build-context df-data-governance=/home/ec2-user/workspaces/df-data-governance \
	--build-arg PYTHON_VERSION=${PYTHON_VERSION} \
	--build-arg CONTAINER_PORT=${CONTAINER_PORT} \
	--build-arg HADOOP_AWS_VERSION=${HADOOP_AWS_VERSION} \
	--build-arg AWS_JAVA_SDK_VERSION=${AWS_JAVA_SDK_VERSION} \
	-t ${IMAGE}:${IMAGE_TAG} .

build-clean-image:
	docker build \
	--build-context utils=/home/ec2-user/workspaces/utils \
	--build-context df-metadata=/home/ec2-user/workspaces/df-metadata \
	--build-context df-app-calendar=/home/ec2-user/workspaces/df-app-calendar \
	--build-context df-config=/home/ec2-user/workspaces/df-config \
	--build-context df-data-governance=/home/ec2-user/workspaces/df-data-governance \
	--build-arg PYTHON_VERSION=${PYTHON_VERSION} \
	--build-arg CONTAINER_PORT=${CONTAINER_PORT} \
	--build-arg HADOOP_AWS_VERSION=${HADOOP_AWS_VERSION} \
	--build-arg AWS_JAVA_SDK_VERSION=${AWS_JAVA_SDK_VERSION} \
	--no-cache \
	-t ${IMAGE}:${IMAGE_TAG} .
# --no-cache forces docker to rebuild all layers from scratch

run-container:
	docker run \
	--mount=type=bind,src=/home/ec2-user/workspaces/nas,dst=/nas \
	-p ${HOST_PORT}:${CONTAINER_PORT} \
	-t -i ${IMAGE}:${IMAGE_TAG}

# AWS 
AWS_ECR := public.ecr.aws/d0h7o5k8
AWS_ECR_REPO_NAMESPACE := dexplorer
AWS_ECR_REPO := dexplorer/df-data-ingestion

aws-auth-to-ecr:
	aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/d0h7o5k8

aws-tag-image:
	docker tag ${IMAGE}:${IMAGE_TAG} ${AWS_ECR}/${AWS_ECR_REPO}:${IMAGE_TAG}

aws-push-image:
	docker push ${AWS_ECR}/${AWS_ECR_REPO}:${IMAGE_TAG}

aws-all: aws-auth-to-ecr aws-build-image aws-tag-image aws-push-image
