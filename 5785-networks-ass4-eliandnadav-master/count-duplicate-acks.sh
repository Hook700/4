#!/bin/bash

# Check if the file path is provided as an argument
if [ $# -ne 3 ]; then
    echo "Usage: $0 <file> <packet#> <count>"
    exit 1
fi

# Count the occurrences of the word "Acked: " in the file
count=$(grep -c "Acked: $2" "$1")

# Compare the count with provided value
if [ "$count" -gt $3 ]; then
    echo "true"
else
    echo "false"
fi

