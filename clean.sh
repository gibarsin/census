#!/bin/bash

SIDE=${1}
CUSTOM_DIR=${2}

if [ -z "${SIDE}" ] || ([ ${SIDE} != "client" ] && [ ${SIDE} != "server" ]); then
    echo "First parameter should be one of 'client' or 'server'"
    exit 1
fi

DEFAULT_DIR="${HOME}/census-${SIDE}"
SCRIPT_DIR=${CUSTOM_DIR:-${DEFAULT_DIR}}

if [ -d ${SCRIPT_DIR} ]; then
    rm -r ${SCRIPT_DIR}
    echo "[DONE] Directory '${SCRIPT_DIR}' cleaned."
else
    echo "[INFO] Directory '${SCRIPT_DIR}' not found. Skipping..."
fi
