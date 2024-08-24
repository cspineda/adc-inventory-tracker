#!/bin/bash

docker build -t lambda-docker-adc-shopify-inventory-forecaster .
docker run -p 9000:8080 --env-file ./.env lambda-docker-adc-shopify-inventory-forecaster