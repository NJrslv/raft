#!/bin/bash

files=("state#0.json" "state#1.json" "state#2.json")

for file in "${files[@]}"; do
    if [ -f "$file" ]; then
        truncate -s 0 "$file"
        echo "Flushed $file"
    else
        echo "$file does not exist"
    fi
done
