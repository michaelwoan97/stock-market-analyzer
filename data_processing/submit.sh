#!/bin/bash

# Default path to the PostgreSQL JAR file
default_jar_path="drivers/postgresql-42.6.0.jar"

# Check if at least one argument is provided
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <python_script> [additional_arguments]"
    exit 1
fi

# Extract the first argument as the Python script
python_script="$1"
shift

# Run spark-submit with the default JAR and specified Python script and additional arguments
spark-submit --jars "$default_jar_path" "$python_script" "$@"
