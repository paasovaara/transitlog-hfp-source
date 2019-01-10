#!/bin/bash
#PSQL_CMD="psql -h localhost -p 54321 -U postgres template1"
#POSTGRES_PASSWORD='transitlog'
#PSQL_CMD="PGPASSWORD=$ADMIN_PASSWORD psql -h localhost -p 54321 -U postgres"
PSQL_CMD="psql -h localhost -p 54321 -U postgres"
$PSQL_CMD -f init.sql template1
