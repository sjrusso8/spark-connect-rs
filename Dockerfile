FROM openjdk:11

# Install git
RUN apt-get update && apt-get install -y git

# Clone a fork of the repository
ARG REPO_URL=https://github.com/sjrusso8/unitycatalog.git
ARG REPO_BRANCH=main
RUN git clone -b $REPO_BRANCH $REPO_URL /usr/src/ucserver

WORKDIR /usr/src/ucserver

# List files for debugging reasons
RUN ls -l .

ARG SBT_VERSION=1.6.2

RUN \
  mkdir /working/ && \
  cd /working/ && \
  curl -L -o sbt-$SBT_VERSION.deb https://repo.scala-sbt.org/scalasbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb && \
  apt-get update && \
  apt-get install sbt && \
  cd && \
  rm -r /working/ && \
  sbt sbtVersion
RUN sbt package

# From the tutorial in the repo
RUN cp -r ./etc/data/external/unity/default/tables/marksheet_uniform /tmp/marksheet_uniform

EXPOSE 8080
ENTRYPOINT /usr/src/ucserver/bin/start-uc-server
