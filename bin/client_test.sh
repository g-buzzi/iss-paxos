#!/usr/bin/env bash

PID_FILE=client.pid

PID=$(cat "${PID_FILE}");

if [ -z "${PID}" ]; then
    echo "Process id for clients is written to location: {$PID_FILE}"
    go build ../client/
    ./client -id 1.1 -algorithm iss -config config_3.json &
    echo $! >> ${PID_FILE}
    ./client -id 1.2 -algorithm iss -config config_3.json &
    echo $! >> ${PID_FILE}
    ./client -id 1.3 -algorithm iss -config config_3.json &
    echo $! >> ${PID_FILE}
    ./client -id 1.4 -algorithm iss -config config_3.json &
    echo $! >> ${PID_FILE}
else
    echo "Clients are already started in this folder."
    exit 0
fi