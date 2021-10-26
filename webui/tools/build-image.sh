#!/bin/sh

set -e

CURRENT_DIR=$(pwd)
JAR_VERSION="0.0.1-SNAPSHOT"
PROJECT_ROOT="$(dirname "${0}")/.."
TOOLS_ROOT="${PROJECT_ROOT}/tools"
IMAGE_VERSION="v0.1"

# server phase
SERVER_SRC_ROOT="${PROJECT_ROOT}/server"
SERVER_DOCKER_ROOT="${TOOLS_ROOT}/server"
SERVER_DOCKER_FILE_PATH="${SERVER_DOCKER_ROOT}/Dockerfile"
JAR_NAME="server-${JAR_VERSION}.jar"
JAR_FILE_FULL="${SERVER_SRC_ROOT}/target/${JAR_NAME}"

cd ${SERVER_SRC_ROOT}

echo "Build in ${SERVER_SRC_ROOT}"
mvn -T1C -DskipTests=true clean package

echo "Install jar to docker path"
cp ${JAR_FILE_FULL} ${SERVER_DOCKER_ROOT}

echo "Build docker image in ${DOCKER_FILE_PATH}"
if [ -f ${SERVER_DOCKER_ROOT}/${JAR_NAME} ]
then
    docker build --build-arg JAR_FILE=${JAR_NAME} -t alink_server:${IMAGE_VERSION} \
           -f ${SERVER_DOCKER_FILE_PATH} ${SERVER_DOCKER_ROOT}
else
    echo "Please build server project first"
fi

# web phase
WEB_SRC_ROOT="${PROJECT_ROOT}/web"
WEB_DOCKER_ROOT="${TOOLS_ROOT}/web"
WEB_DOCKER_FILE_PATH="${WEB_DOCKER_ROOT}/Dockerfile"
WEB_DIST_FOLDER="dist"
WEB_DIST_SRC_FOLDER_FULL="${WEB_SRC_ROOT}/${WEB_DIST_FOLDER}"
WEB_DIST_TOOLS_FOLDER_FULL="${WEB_DOCKER_ROOT}/${WEB_DIST_FOLDER}"

echo "Build in ${PROJECT_ROOT}"
cd ${WEB_SRC_ROOT}
yarn build

echo "Install dist to docker path"
if [ -d ${WEB_DIST_TOOLS_FOLDER_FULL} ]
then
    rm -rf ${WEB_DIST_TOOLS_FOLDER_FULL}
fi

cp -r ${WEB_DIST_SRC_FOLDER_FULL} ${WEB_DIST_TOOLS_FOLDER_FULL}

echo "Build docker image in ${DOCKER_FILE_PATH}"

if [ -d ${WEB_DIST_TOOLS_FOLDER_FULL} ]
then
    docker build --build-arg DIST_FOLDER=${WEB_DIST_FOLDER} -t alink_web:${IMAGE_VERSION} \
           -f ${WEB_DOCKER_FILE_PATH} ${WEB_DOCKER_ROOT}
else
    echo "Please build web project first"
fi

# flink-with-alink-jar phase
FLINK_WITH_ALINK_JAR_DOCKER_ROOT="${TOOLS_ROOT}/flink-with-alink-jar"
ALINK_CORE_JAR_PATH=$FLINK_WITH_ALINK_JAR_DOCKER_ROOT/alink_core_flink-1.9_2.11-1.5.0.jar
FLINK_WITH_ALINK_JAR_DOCKER_FILE_PATH="${FLINK_WITH_ALINK_JAR_DOCKER_ROOT}/Dockerfile"

if [ -f "${ALINK_CORE_JAR_PATH}" ]
then
    docker build -t flink_with_alink_jar:${IMAGE_VERSION} \
           -f "${FLINK_WITH_ALINK_JAR_DOCKER_FILE_PATH}" "${FLINK_WITH_ALINK_JAR_DOCKER_ROOT}"
else
  echo "Please provide alink_core jar file"
fi

# jupyter-notebook
NOTEBOOK_DOCKER_ROOT="${TOOLS_ROOT}/jupyter"
NOTEBOOK_DOCKER_FILE_PATH="${NOTEBOOK_DOCKER_ROOT}/Dockerfile"

docker build -t alink_notebook:${IMAGE_VERSION} -f ${NOTEBOOK_DOCKER_FILE_PATH} ${NOTEBOOK_DOCKER_ROOT}

#nfs
NFS_DOCKER_ROOT="${TOOLS_ROOT}/nfs"
NFS_DOCKER_FILE_PATH="${NFS_DOCKER_ROOT}/Dockerfile"

docker build -t alink_nfs:${IMAGE_VERSION} -f ${NFS_DOCKER_FILE_PATH} ${NFS_DOCKER_ROOT}

cd ${CURRENT_DIR}
