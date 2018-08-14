#!/usr/bin/env bash

flask db upgrade
gunicorn -b 0.0.0.0:5555 --workers 1 --log-level warning --timeout 240 --threads 4 --worker-class gthread --reload --access-logfile - --name restserver start:app