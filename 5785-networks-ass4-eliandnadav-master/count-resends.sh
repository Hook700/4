#!/bin/bash

# Check if the file path is provided as an argument
if [ $# -ne 2 ]; then
    echo "Usage: $0 <file> <resendCount>"
    exit 1
fi

# Count the occurrences of the word "Resent" in the file
count=$(grep -c 'Resent' "$1")

# Compare the count with provided value
if [ "$count" -gt $2 ]; then
    echo "true"
else
    echo "false"
fi

