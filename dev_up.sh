#!/usr/bin/env bash
export current_branch=`git rev-parse --symbolic-full-name --abbrev-ref HEAD`
docker-compose up -d
