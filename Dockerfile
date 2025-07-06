# syntax=docker.io/docker/dockerfile:1.7-labs

# Base stage - build dependencies

FROM public.ecr.aws/amazonlinux/amazonlinux:latest as builder

# Update installed packages and install system dependencies
RUN dnf update -y \
    && dnf install -y make \
    && dnf install -y wget 

ARG PYTHON_VERSION

# Install python and pip 
RUN dnf install python${PYTHON_VERSION} -y \
    && dnf install python${PYTHON_VERSION}-pip -y

# Create Python Virtual Env
RUN python${PYTHON_VERSION} -m venv /venv

# Create and set work directory (make/pip need this to find Makefile and pyproject.toml)
WORKDIR /df-data-ingestion

# Copy the application dependencies
COPY ./pyproject.toml /df-data-ingestion
COPY ./Makefile /df-data-ingestion

# Copy the app dependency source code into the container.
COPY --from=utils . /packages/utils
COPY --from=df-metadata . /packages/df-metadata
COPY --from=df-app-calendar . /packages/df-app-calendar
COPY --from=df-config --exclude=./cfg . /packages/df-config
COPY --from=df-data-governance . /packages/df-data-governance

# Install app dependencies
RUN source /venv/bin/activate \
    && make install

# Add hadoop-aws for spark-aws integration
ARG HADOOP_AWS_VERSION
ARG AWS_JAVA_SDK_VERSION
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -P /venv/lib/python${PYTHON_VERSION}/site-packages/pyspark/jars \
    && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_JAVA_SDK_VERSION}/aws-java-sdk-bundle-${AWS_JAVA_SDK_VERSION}.jar -P /venv/lib/python${PYTHON_VERSION}/site-packages/pyspark/jars

# RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -P /venv/lib/python${PYTHON_VERSION}/site-packages/pyspark/jars \
# && wget https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_JAVA_SDK_VERSION}/bundle-${AWS_JAVA_SDK_VERSION}.jar -P /venv/lib/python${PYTHON_VERSION}/site-packages/pyspark/jars

# Copy the app source code into the container.
COPY --exclude=*.env . /df-data-ingestion

# Install app 
RUN source /venv/bin/activate \
    && make install 


# Final stage

FROM public.ecr.aws/amazonlinux/amazonlinux:latest as runner 

# Update installed packages and install system dependencies
RUN dnf update -y \
    && dnf install -y findutils \
    && dnf install -y tree \
    && dnf install -y shadow-utils \
    && dnf install -y java-17-amazon-corretto \
    && dnf install -y procps
# findutils is needed for xargs command
# shadow-utils is needed for useradd command
# java-17-amazon-corretto, procps are needed for pyspark

ARG PYTHON_VERSION

# Install python and pip 
RUN dnf install python${PYTHON_VERSION} -y \
    && dnf install python${PYTHON_VERSION}-pip -y

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PATH="/venv/bin:$PATH"
ENV AWS_JAVA_V1_DISABLE_DEPRECATION_ANNOUNCEMENT=true

COPY --from=builder /venv /venv

# Copy the app source code from current directory (where Dockerfile is) into the container.
COPY --exclude=*.env . /df-data-ingestion
COPY --from=df-config ./cfg /packages/df-config/cfg

# Expose the port that the application listens on. i.e. container port
# This is just for documentation. Port should be exposed when the container is instantiated or via docker publish port.
ARG CONTAINER_PORT
EXPOSE ${CONTAINER_PORT}

# Set work directory for the app
WORKDIR /df-data-ingestion

# Create a non-privileged user that the app will run under.
RUN useradd --create-home --shell /bin/bash app-user

# Switch to the non-privileged user to run the application.
USER app-user

# Run the application.
CMD ["/bin/bash"]
# CMD ["dq-app-api"]
# CMD ["dq-app-api", "--debug"]
# CMD ["dq-app-api", "--debug", "y"]
