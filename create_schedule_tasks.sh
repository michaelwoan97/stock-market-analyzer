#!/bin/bash

# Change to the child directory where your modules are located using a relative path
cd "schedule-tasks"

echo "Running the first command..."
# Run the first command in a separate terminal and display the output
start cmd /c "celery -A tasks worker --pool=solo -l info"

echo "Running the second command..."
# Run the second command in a separate terminal and display the output
start cmd /c "celery -A celery_schedule beat --loglevel=info"
