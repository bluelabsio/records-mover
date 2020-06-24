#!/bin/bash -e

while ! db dockerized-mysql </dev/null
do
  >&2 echo "Waiting 5 seconds for MySQL to be available"
  sleep 5
done
