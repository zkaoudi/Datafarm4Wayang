FROM maven:3.9.4-eclipse-temurin-11-focal

USER root

WORKDIR /usr/src/app

RUN apt update && apt-get install -y \
    apt-transport-https \
    curl \
    gnupg \
    libsnappy-dev \
    libzip-dev \
    openssl \
    python \
    pip \
    python3-dev \
    graphviz \
    libgraphviz-dev \
    pkg-config \
    python3.8-venv

# Install Scala kernel
ENV SCALA_VERSION=2.11.4 \
    SCALA_HOME=/usr/share/scala

# NOTE: bash is used by scala/scalac scripts, and it cannot be easily replaced with ash.
RUN apt-get install -y wget ca-certificates && \
    apt-get install -y bash jq && \
    cd "/tmp" && \
    wget --no-verbose "https://downloads.typesafe.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.tgz" && \
    tar xzf "scala-${SCALA_VERSION}.tgz" && \
    mkdir "${SCALA_HOME}" && \
    rm "/tmp/scala-${SCALA_VERSION}/bin/"*.bat && \
    mv "/tmp/scala-${SCALA_VERSION}/bin" "/tmp/scala-${SCALA_VERSION}/lib" "${SCALA_HOME}" && \
    ln -s "${SCALA_HOME}/bin/"* "/usr/bin/" && \
    rm -rf "/tmp/"*

# Install sbt
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import && \
chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg && \
apt-get update && \
apt-get install sbt

ENTRYPOINT "/bin/bash"
