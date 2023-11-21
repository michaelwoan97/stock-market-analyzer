#!/bin/bash

# Change to the child directory where your modules are located using a relative path
cd "schedule-tasks"

echo "Launching Celery Worker..."
# Run the first command in a separate terminal and display the output
gnome-terminal --tab --title="Celery Worker" --command="bash -c 'celery -A tasks worker --pool=solo -l info; $SHELL'"

echo "Launching Celery Beat..."
# Run the second command in a separate terminal and display the output
gnome-terminal --tab --title="Celery Beat" --command="bash -c 'celery -A celery_schedule beat --loglevel=info; $SHELL'"
