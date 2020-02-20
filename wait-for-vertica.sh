#!/bin/bash -e

while ! db dockerized-vertica </dev/null
do
  >&2 echo "Waiting 5 seconds for Vertica to be available"
  sleep 5
done
