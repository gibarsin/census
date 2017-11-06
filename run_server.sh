#!/bin/bash

SERVER_DIR=${1:-$HOME/census-server}
SERVER_PACKAGE_TAR="server/target/census-server-1.0-SNAPSHOT-bin.tar.gz"

if [ ! -d ${SERVER_DIR} ]; then
    mkdir -p ${SERVER_DIR}
    if [ ! -f ${SERVER_PACKAGE_TAR} ]; then
        mvn clean package
    fi
    tar xzf ${SERVER_PACKAGE_TAR} -C ${SERVER_DIR} --strip-components=1
fi

cd ${SERVER_DIR}
./run-server.sh
