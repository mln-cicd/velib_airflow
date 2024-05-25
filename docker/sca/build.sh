#!/bin/bash
SCRIPT_DIR=$(dirname "$0")
echo "SCRIPT_DIR: ${SCRIPT_DIR}"
cd "$SCRIPT_DIR"

# Get the name of the containing folder
FOLDER_NAME=$(basename "$PWD")
echo "FOLDER_NAME: ${FOLDER_NAME}"
# Get the Docker Hub account name from the command line argument
DOCKER_ACCOUNT=$1

# Check if the Docker Hub account name is provided
if [ -z "$DOCKER_ACCOUNT" ]; then
    echo "Please provide a Docker Hub account name as an argument."
    exit 1
fi

mkdir -p ./app
# Copy app files to the build context
cp ../../main.py .
cp -r ../../app/ .
echo "Checking the '/app' content brought to image build context:"
ls ./app
# Build the Docker image with the folder name as the tag and the provided Docker Hub account name
docker build -t "${DOCKER_ACCOUNT}/velib:${FOLDER_NAME}" -f "Dockerfile.${FOLDER_NAME}" .

# Cleanup: Remove copied files
rm -rf ./app
rm -rf ./main.py