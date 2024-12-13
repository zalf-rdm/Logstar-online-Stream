#!/bin/bash

cmd=(python /Logstar-online-Stream/logstar-receiver.py)
if [ "$LOGSTAR_DEBUG" = true ]; then
  cmd+=(-v)
fi
if [[ -v LOGSTAR_LOGFILE ]]; then
  cmd+=(-log "$LOGSTAR_LOGFILE")
fi
cmd+=("$LOGSTAR_PARAMS")
"${cmd[@]}"