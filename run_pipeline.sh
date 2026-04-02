#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

PYTHON="/home/aswinsairam/airflow_env/bin/python3"

echo "Running ingest..."
"$PYTHON" ~/ingest_linux.py

echo "Running transform..."
"$PYTHON" ~/transform_linux.py

echo "Running gold..."
"$PYTHON" ~/gold_linux.py

echo "Pipeline complete!"
