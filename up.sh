#!/usr/bin/env bash
docker-compose config > docker-compose-parsed.yaml
docker stack deploy -c ./docker-compose-parsed.yaml SMFR
docker service update SMFR_geonames --detach=false
docker service update SMFR_annotator --detach=false
docker service update SMFR_geocoder --detach=false
docker service update SMFR_restserver --detach=false
docker service update SMFR_web --detach=false
