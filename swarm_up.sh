#!/usr/bin/env bash
export image_tag=`cat VERSION | grep "VERSION" | cut -d'=' -f2`

docker-compose config > docker-compose-parsed.yaml

# cleaning volumes from docker compose configuration
python3 compose4deploy.py -i docker-compose-parsed.yaml -o docker-compose-4deploy.yaml
docker stack deploy -c ./docker-compose-4deploy.yaml SMFR

# forcing updates of images
docker service update SMFR_persister --detach=false --image efas/persister:${image_tag}
docker service update SMFR_geonames --detach=false --image efas/geonames:${image_tag}
docker service update SMFR_mysql --detach=false --image efas/mysql:${image_tag}
docker service update SMFR_annotator --detach=false --image efas/annotator:${image_tag}
docker service update SMFR_geocoder --detach=false --image efas/geocoder:${image_tag}
docker service update SMFR_restserver --detach=false --image efas/restserver:${image_tag}
docker service update SMFR_web --detach=false --image efas/web:${image_tag}
