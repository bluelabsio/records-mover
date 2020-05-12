#!/bin/bash -e

while ! db dockerized-postgres </dev/null
do
  >&2 echo "Waiting 5 seconds for Postgres to be available"
  sleep 5
done
