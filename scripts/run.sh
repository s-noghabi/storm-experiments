#!/bin/bash
#
# Run the storm experiments

storm jar ./target/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm.starter.TestTopology $1
