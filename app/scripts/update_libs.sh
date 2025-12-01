#!/bin/bash

# Script to build the local CPQ-native-index fork and copy the jar to libs/
# Usage: ./scripts/update_libs.sh [path_to_cpq_repo]

CPQ_PATH="${1:-/home/dazai/projects/CPQ-native-index/CPQ-native Index}"
LIBS_DIR="$(pwd)/libs"

if [ ! -d "$CPQ_PATH" ]; then
    echo "Error: CPQ-native-index directory not found at $CPQ_PATH"
    echo "Usage: $0 [path_to_cpq_repo]"
    exit 1
fi

echo "Building CPQ-native-index in $CPQ_PATH..."
cd "$CPQ_PATH" || exit

# Assume gradle project
if [ -f "./gradlew" ]; then
    ./gradlew clean assemble
else
    echo "Error: gradlew not found in $CPQ_PATH"
    exit 1
fi

# Find the jar
JAR_FILE=$(find build/libs -name "*.jar" | grep -v "plain" | grep -v "javadoc" | grep -v "sources" | head -n 1)

if [ -z "$JAR_FILE" ]; then
    echo "Error: Could not find built jar file in $(pwd)/build/libs"
    echo "Contents of build/libs:"
    ls -l build/libs
    exit 1
fi

echo "Found jar: $JAR_FILE"
TARGET_NAME="cpq-native-index-1.0.jar"
cp "$JAR_FILE" "$LIBS_DIR/$TARGET_NAME"

echo "Successfully copied $(basename "$JAR_FILE") to $LIBS_DIR/$TARGET_NAME"
