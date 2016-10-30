#!/bin/bash
docker run --net docker_default -v $(pwd)/../target:/target confluentinc/cp-base:latest java -cp /target/changesmonitor-1.0-SNAPSHOT-standalone.jar org.cmatta.kafka.streams.wikipedia.MessageMonitor
