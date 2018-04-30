#!/usr/bin/env bash
docker-compose config > docker-compose-parsed.yaml
docker stack deploy -c ./docker-compose-parsed.yaml SMFR
docker service update SMFR_geonames
docker service update SMFR_annotator
docker service update SMFR_geocoder
docker service update SMFR_restserver
docker service update SMFR_web
