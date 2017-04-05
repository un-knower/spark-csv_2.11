#!/usr/bin/env bash

# deploy

mvn clean package -DskipTests "$@"
