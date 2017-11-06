#!/bin/bash


SIDE=${1}
CUSTOM_DIR=${2}

if [ -z "${SIDE}" ] || ([ ${SIDE} != "client" ] && [ ${SIDE} != "server" ]); then
    echo "First parameter should be one of 'client' or 'server'"
    exit 1
fi

DEFAULT_DIR="${HOME}/census-${SIDE}"
SCRIPT_DIR=${CUSTOM_DIR:-${DEFAULT_DIR}}
SCRIPT_PACKAGE_TAR="${SIDE}/target/census-${SIDE}-1.0-SNAPSHOT-bin.tar.gz"
RUN_FILE="run-${SIDE}.sh"

if [ ! -d ${SCRIPT_DIR} ]; then
    mkdir -p ${SCRIPT_DIR}
fi

if [ ! -f ${SCRIPT_DIR}/${RUN_FILE} ]; then
    if [ ! -f ${SCRIPT_PACKAGE_TAR} ]; then
        mvn clean package
    fi
    tar xzf ${SCRIPT_PACKAGE_TAR} -C ${SCRIPT_DIR} --strip-components=1
fi

cd ${SCRIPT_DIR}
./${RUN_FILE}
