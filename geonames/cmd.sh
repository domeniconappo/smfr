#!/usr/bin/env bash
#echo "running elastic"
#su elasticsearch -c elasticsearch
#sleep 5
#exec curl -L -XPUT 'http://127.0.0.1:9200/_all/_settings' -d '{"action.auto_create_index":"true", "index.mapper.dynamic":"true"}'
#exec curl -L -XPUT 'http://127.0.0.1:9200/geonames' -d '{"index":{"analysis":{"filter":{"ngram":{"type":"ngram","min_gram":2,"max_gram":15}}}}}'
