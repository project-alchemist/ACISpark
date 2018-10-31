#!/bin/bash
set -o errexit

source ./config.sh

export ACISPARK_JAR=$ACISPARK_PATH/target/scala-2.11/alchemist-assembly-0.5.jar

CURR_DIR=$PWD

echo " "
cd $ACISPARK_PATH
echo "Building Alchemist-Spark Interface for $SYSTEM"
LINE="======================================="
for i in `seq 1 ${#SYSTEM}`;
do
	LINE="$LINE="
done
echo $LINE
echo " "
echo "Creating Alchemist-Spark Interface JAR file:"
echo " "
sbt -batch assembly
echo " "
echo $LINE
echo " "
echo "Building process for Alchemist-Spark Interface has completed"
echo " "
echo "If no issues occurred during build:"
echo "  Alchemist-Spark Interface JAR file located at:      $ACISPARK_JAR"
echo " "
echo "  Run './start.sh' to start Alchemist-Spark Interface"
echo " "
cd $CURR_DIR
