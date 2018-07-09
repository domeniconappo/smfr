#!/usr/bin/env bash
export current_branch=`git rev-parse --symbolic-full-name --abbrev-ref HEAD`
docker-compose config > docker-compose-parsed.yaml

# cleaning volumes from docker compose configuration
python3 compose4deploy.py -i docker-compose-parsed.yaml -o docker-compose-4deploy.yaml
docker stack deploy -c ./docker-compose-4deploy.yaml SMFR

# forcing updates of images
docker service update SMFR_persister --detach=false --image efas/persister:${current_branch}
docker service update SMFR_geonames --detach=false --image efas/geonames:latest
docker service update SMFR_mysql --detach=false --image efas/mysql:latest
docker service update SMFR_annotator --detach=false --image efas/annotator:${current_branch}
docker service update SMFR_geocoder --detach=false --image efas/geocoder:${current_branch}
docker service update SMFR_restserver --detach=false --image efas/restserver:${current_branch}
docker service update SMFR_web --detach=false --image efas/web:${current_branch}
