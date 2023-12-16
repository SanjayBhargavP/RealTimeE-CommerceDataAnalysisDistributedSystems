#!/bin/bash
# Generate and register instance_id
python generate_instance_id.py > /tmp/instance_id.txt
instance_id=$(cat /tmp/instance_id.txt)
# Wait for a few seconds to ensure synchronization
sleep 30
python get_total_instances.py > /tmp/total_instances.txt
total_instances=$(cat /tmp/total_instances.txt)
python dataIngestion.py --instance_id $instance_id  --total_instances $total_instances
