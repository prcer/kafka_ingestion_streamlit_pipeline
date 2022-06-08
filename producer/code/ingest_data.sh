#! /bin/bash

#echo Waiting for Kafka to be ready...
#cub kafka-ready -b broker:29092 1 40
#echo Waiting for Confluent Schema Registry to be ready...
#cub sr-ready schema-registry 8081 40

# TODO Define correct command to wait for connect at port 8083 (curl with retries?) instead of waiting for 2 min
echo Waiting 2 min before starting ingestion...
sleep 120
echo Starting ingestion
# Run command to create topic and start sending CSV data to Kafka (enforcing a schema)
curl -i -X PUT -H "Accept:application/json" \
               -H  "Content-Type:application/json" http://connect:8083/connectors/source-csv-spooldir-00/config \
               -d '{
                    "connector.class": "com.github.jcustenborder.kafka.connect.spooldir.SpoolDirCsvSourceConnector",
                    "topic": "science",
                    "input.path": "/data/unprocessed",
                    "finished.path": "/data/processed",
                    "error.path": "/data/error",
                    "input.file.pattern": ".*\\.csv",
                    "schema.generation.enabled":"true",
                    "csv.first.row.as.header":"true"
                   }'
tail -f /dev/null