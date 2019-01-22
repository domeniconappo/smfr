#!/usr/bin/env bash
export SINGLENODE=0
docker-compose -f docker-compose.dbs.yaml -f docker-compose.yaml stop
