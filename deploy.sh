#!/usr/bin/env bash
docker-compose config > docker-compose-parsed.yaml
docker stack deploy -c ./docker-compose-parsed.yaml SMFR
