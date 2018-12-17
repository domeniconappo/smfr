#!/usr/bin/env bash

# Add local user
# Either use the LOCAL_USER_ID if passed in at runtime or
# fallback

USER_ID=${LOCAL_USER_ID:-1001}

echo "Starting with UID : $USER_ID"
adduser -D -u ${USER_ID} user
export HOME=/home/user
exec /usr/local/bin/gosu user "$@"
