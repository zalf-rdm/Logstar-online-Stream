#!/bin/bash

# handle debug mode (-v)
if [[ -v LOGSTAR_DEBUG ]]; then
    LOGSTAR_DEBUG="-v"
 else
    LOGSTAR_DEBUG=""
fi

# handle logging to file (-log)
if [[ -v LOGSTAR_LOGFILE ]]; then
    LOGSTAR_LOGGING="-log $LOGSTAR_LOGFILE"
else
    LOGSTAR_LOGGING=""

python /Logstar-online-Stream/logstar-receiver.py $LOGSTAR_DEBUG $LOGSTAR_LOGGING $LOGSTAR_PARAMS

