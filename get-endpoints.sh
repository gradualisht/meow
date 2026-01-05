#!/bin/bash

# holt alle config endpoints vom config-server 
curl http://localhost:8000/endpoints > all-endpoints.json
