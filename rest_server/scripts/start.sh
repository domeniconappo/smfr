##!/usr/bin/env bash
#_term1() {
#  echo "Caught SIGTERM signal!"
#  kill -s TERM "$child" 2>/dev/null
#  echo "Forwarded SIGTERM to master gunicorn ${child}"
#}
#
#_term2() {
#  echo "Caught SIGINT signal!"
#  kill -s TERM "$child" 2>/dev/null
#  echo "Forwarded SIGTERM to master gunicorn ${child}"
#}
#
#_term3() {
#  echo "Caught SIGQUIT signal!"
#  kill -s TERM "$child" 2>/dev/null
#  echo "Forwarded SIGTERM to master gunicorn ${child}"
#}
#
#trap _term1 SIGTERM
#trap _term2 SIGINT
#trap _term3 SIGQUIT
#
#/scripts/wait_for_it.sh cassandra:9042 -- gunicorn -b 0.0.0.0:5555 --workers 1 --threads 4 --worker-class gthread --reload --access-logfile - --name restserver "start:app" &
#
#child=$!
#echo "Master gunicorn pid: ${child}"
#wait "$child"