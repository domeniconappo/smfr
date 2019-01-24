#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source ${DIR}/export_env.sh
export PYTHONSTARTUP=${DIR}/startup_shell.py
ipython
