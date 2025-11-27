#!/bin/bash

# Function: Compile project, run JaCoCo coverage analysis and generate report

echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] Running coverage analysis"
echo "[INFO] ------------------------------------------------------------------------"

# Configure variables
JACOCO_AGENT="jacoco/lib/jacocoagent.jar"
JACOCO_CLI="jacoco/lib/jacococli.jar"
APP_JAR="details/target/details-master.jar"
EXEC_FILE="jacoco.exec"
CLASS_FILES="code/classes"
SOURCE_FILES="code/src"
REPORT_DIR="jacoco/report"

# Check necessary files and directories
if [ ! -d "jacoco/lib" ]; then
    echo "[ERROR] Directory jacoco/lib does not exist"
    exit 1
fi

if [ ! -f "$JACOCO_AGENT" ]; then
    echo "[ERROR] File $JACOCO_AGENT does not exist"
    exit 1
fi

if [ ! -f "$JACOCO_CLI" ]; then
    echo "[ERROR] File $JACOCO_CLI does not exist"
    exit 1
fi

if [ -d "$CLASS_FILES" ]; then
   rm -f "$CLASS_FILES"/*
   rm -rf "$CLASS_FILES"/*
else
    mkdir -p "$CLASS_FILES"
fi

if [ ! -d "$SOURCE_FILES" ]; then
    echo "[ERROR] Source directory $SOURCE_FILES does not exist"
    exit 1
fi

if [ ! -d "$SOURCE_FILES/org/apache/iotdb" ]; then
    echo "[ERROR] Source code does not exist, please supplement it before execution"
    exit 1
fi

mvn clean package -DskipTests
if [ $? -ne 0 ]; then
    echo "[ERROR] Maven build failed"
    exit 1
fi

sleep 2

# Extract org.apache class files from jar and move to class files directory
echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] Extracting classes from JAR: $APP_JAR"
echo "[INFO] ------------------------------------------------------------------------"
jar -xf "$APP_JAR" org/

if [ -d "org/apache/iotdb" ]; then
    cp -r "org/apache/iotdb" "$CLASS_FILES/org/apache/iotdb/"
fi

if [ -d "org/apache/tsfile" ]; then
    cp -r "org/apache/tsfile" "$CLASS_FILES/org/apache/tsfile/"
fi

find "$CLASS_FILES" -type d -name "filter" -exec rm -rf {} +
find "$CLASS_FILES" -type d -name "subscription" -exec rm -rf {} +
find "$CLASS_FILES" -type d -name "template" -exec rm -rf {} +

# Delete isession/pool directory
if [ -d "code/src/org/apache/iotdb/isession/pool" ]; then
    rm -rf "code/src/org/apache/iotdb/isession/pool"
fi

# Clean up temporary directory
if [ -d "org" ]; then
    rm -rf org
fi

# Check if IoTDB and TsFile class directories exist in the class files directory
if [ ! -d "$CLASS_FILES/org/apache/iotdb" ]; then
    echo "[WARN]   - IoTDB classes NOT found"
    exit 1
fi

if [ ! -d "$CLASS_FILES/org/apache/tsfile" ]; then
    echo "[WARN]   - TsFile classes NOT found"
    exit 1
fi

echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] Running application and collecting coverage data"
echo "[INFO] ------------------------------------------------------------------------"

# Run application and collect coverage data
java -javaagent:$JACOCO_AGENT=includes=org.apache.\*,output=file,destfile=$EXEC_FILE,append=false -jar $APP_JAR

# Add delay waiting to ensure execution completion
sleep 5

# Check if execution data file exists
if [ ! -f "$EXEC_FILE" ]; then
    echo "[ERROR] Execution data file $EXEC_FILE was not generated"
    exit 1
fi

echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] Generating coverage report"
echo "[INFO] ------------------------------------------------------------------------"

# Check if class files are available for report generation
if [ ! -n "$(find "$CLASS_FILES" -type f)" ]; then
    echo "[ERROR] No class files found in $CLASS_FILES for report generation"
    exit 1
fi

# Generate coverage report
java -jar $JACOCO_CLI report $EXEC_FILE \
  --classfiles $CLASS_FILES/org/apache/iotdb/isession \
  --classfiles $CLASS_FILES/org/apache/iotdb/session \
  --classfiles $CLASS_FILES/org/apache/iotdb/rpc \
  --classfiles $CLASS_FILES/org/apache/tsfile \
  --sourcefiles $SOURCE_FILES \
  --html $REPORT_DIR

# Check if the inspection report was generated successfully
if [ $? -ne 0 ]; then
    echo "[ERROR] Failed to generate coverage report"
    exit 1
fi

echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] Coverage analysis completed!"
echo "[INFO] Report location: $REPORT_DIR/index.html"
echo "[INFO] ------------------------------------------------------------------------"