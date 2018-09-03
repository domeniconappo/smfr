#!/usr/bin/env bash

cd /restserver
flask db upgrade
gunicorn -b 0.0.0.0:5555 --workers 1 --log-level warning --timeout 240 --threads 4 -k gthread --reload --access-logfile - --name restserver start:app
