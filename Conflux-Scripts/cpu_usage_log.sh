#!/bin/bash

PNAME="$1"
LOG_FILE="$2"

echo "" > $LOG_FILE

while true ; do
    echo "$(date +%H:%M:%S) :: $PNAME[$(pidof ${PNAME})] $(ps -C ${PNAME} -o %cpu | tail -1)%" >> $LOG_FILE
    sleep 1
done
