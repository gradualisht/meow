#!/bin/bash

# erstellt/ überschreibt endpoint im config-server
curl -X POST \
  -H "Content-Type: application/json" \
  -d @new-endpoint.json \
  http://localhost:8000/endpoints/local-canary-post
