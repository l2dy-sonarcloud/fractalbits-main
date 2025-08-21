#!/bin/bash

STACK_NAME=${1:-FractalbitsVpcStack}

# Get direct EC2 instance IDs from the CloudFormation stack
DIRECT_INSTANCE_IDS=$(aws cloudformation describe-stack-resources --stack-name "$STACK_NAME" --query 'StackResources[?ResourceType==`AWS::EC2::Instance`].PhysicalResourceId' --output text)

# Get Auto Scaling Group names from the CloudFormation stack
ASG_NAMES=$(aws cloudformation describe-stack-resources --stack-name "$STACK_NAME" --query 'StackResources[?ResourceType==`AWS::AutoScaling::AutoScalingGroup`].PhysicalResourceId' --output text)

# Collect all ASG instance IDs
ASG_INSTANCE_IDS=""
if [ -n "$ASG_NAMES" ]; then
    for ASG_NAME in $ASG_NAMES; do
        ASG_INSTANCES=$(aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names "$ASG_NAME" --query 'AutoScalingGroups[].Instances[].InstanceId' --output text)
        if [ -n "$ASG_INSTANCES" ]; then
            ASG_INSTANCE_IDS="$ASG_INSTANCE_IDS $ASG_INSTANCES"
        fi
    done
fi

# Get instances by CloudFormation stack tag
TAGGED_INSTANCE_IDS=$(aws ec2 describe-instances --filters "Name=tag:aws:cloudformation:stack-name,Values=$STACK_NAME" "Name=instance-state-name,Values=pending,running,stopping,stopped" --query 'Reservations[].Instances[].InstanceId' --output text)

# Get instances by Name tag prefix (fallback for instances not tagged with stack name)
NAME_PREFIX_INSTANCE_IDS=$(aws ec2 describe-instances --filters "Name=tag:Name,Values=${STACK_NAME}/*" "Name=instance-state-name,Values=pending,running,stopping,stopped" --query 'Reservations[].Instances[].InstanceId' --output text)

# Combine all instance IDs and remove duplicates
ALL_INSTANCE_IDS=$(echo "$DIRECT_INSTANCE_IDS $ASG_INSTANCE_IDS $TAGGED_INSTANCE_IDS $NAME_PREFIX_INSTANCE_IDS" | tr ' ' '\n' | sort -u | tr '\n' ' ')

# Get zone name to zone ID mapping
declare -A ZONE_MAP
while IFS=$'\t' read -r zone_name zone_id; do
    ZONE_MAP["$zone_name"]="$zone_id"
done < <(aws ec2 describe-availability-zones --query 'AvailabilityZones[].[ZoneName,ZoneId]' --output text)

# Display all instances in a single table
if [ -n "$ALL_INSTANCE_IDS" ]; then
    echo "=== All EC2 Instances in Stack: $STACK_NAME ==="
    echo "Name                                          | InstanceId           | State    | InstanceType    | AvailabilityZone | ZoneId     "
    echo "----------------------------------------------+----------------------+----------+-----------------+------------------+------------"
    aws ec2 describe-instances --instance-ids $ALL_INSTANCE_IDS --query 'Reservations[].Instances[].[Tags[?Key==`Name`]|[0].Value,InstanceId,State.Name,InstanceType,Placement.AvailabilityZone]' --output text | while read -r name id state type az; do
        zone_id="${ZONE_MAP[$az]:-N/A}"
        printf "%-45s | %-20s | %-8s | %-15s | %-16s | %-10s\n" "$name" "$id" "$state" "$type" "$az" "$zone_id"
    done
else
    echo "No EC2 instances found in stack: $STACK_NAME"
fi
