#!/usr/bin/env bash
docker-compose config > docker-compose-parsed.yaml
python3 compose4deploy.py -i docker-compose-parsed.yaml -o docker-compose-4deploy.yaml
docker stack deploy -c ./docker-compose-4deploy.yaml SMFR
docker service update SMFR_persister --detach=false
docker service update SMFR_geonames --detach=false
docker service update SMFR_annotator --detach=false
docker service update SMFR_geocoder --detach=false
docker service update SMFR_restserver --detach=false
docker service update SMFR_web --detach=false
