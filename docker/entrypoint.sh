#!/bin/bash

if [[ -v LOGSTAR_DEBUG ]]; then
    $LOGSTAR_DEBUG="-v"
 else
    $LOGSTAR_DEBUG=""
fi

if [[ -v LOGSTAR_LOGFILE ]]; then
    $LOGSTAR_LOGGING="-log $LOGSTAR_LOGFILE"
else
    $LOGSTAR_LOGGING=""

python /Logstar-online-Stream/logstar-receiver.py $LOGSTAR_DEBUG $LOGSTAR_LOGGING $LOGSTAR_PARAMS


