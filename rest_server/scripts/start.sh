#!/usr/bin/env bash

/bin/bash /scripts/wait_for_it.sh cassandra:9042 -- gunicorn -b 0.0.0.0:5555 --workers 1 --threads 4 --worker-class gthread --reload --access-logfile - --name restserver "start:create_app()"

#export FLASK_APP=smfr.py & flask start_collector
