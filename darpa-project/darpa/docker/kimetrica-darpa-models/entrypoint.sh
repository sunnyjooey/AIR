#!/bin/bash
set -e

echo Using usermod to set UID to ${UID}
# this will allow mounts to work, when the user on the host system has
# a UID other than 1000
usermod -u ${UID} jovyan
echo Starting normal execution as jovyan user
gosu jovyan "$@"
