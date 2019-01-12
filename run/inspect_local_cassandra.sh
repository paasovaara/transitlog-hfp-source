#!/bin/bash
#--link cassandra:cassandra
docker run -it --rm cassandra:3.11.3 sh -c 'exec cqlsh host.docker.internal 9042'
