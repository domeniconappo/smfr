#!/usr/bin/env bash
export current_branch=`git rev-parse --symbolic-full-name --abbrev-ref HEAD`
python compose4build.py ${current_branch}
docker-compose up -d