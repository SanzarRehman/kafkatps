#!/bin/bash

# Docker Registry Configuration
REGISTRY_URL="docker.io"  # Docker Hub registry
DOCKER_USERNAME="sanzar686"
IMAGE_NAME="java-kafka"

# Generate dynamic version using timestamp
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
VERSION="v${TIMESTAMP}"
BASE_VERSION="3.0-SNAPSHOT"

FULL_IMAGE_NAME="${DOCKER_USERNAME}/${IMAGE_NAME}:${VERSION}"
LATEST_IMAGE_NAME="${DOCKER_USERNAME}/${IMAGE_NAME}:latest"
SNAPSHOT_IMAGE_NAME="${DOCKER_USERNAME}/${IMAGE_NAME}:${BASE_VERSION}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Starting Docker build and push process...${NC}"

# Step 1: Build the application
echo -e "${YELLOW}Building application with Gradle...${NC}"
./gradlew clean build
if [ $? -ne 0 ]; then
    echo -e "${RED}Gradle build failed!${NC}"
    exit 1
fi

# Step 2: Copy the JAR file to the root directory for Docker build
echo -e "${YELLOW}Copying JAR file...${NC}"
cp build/libs/kafkatps-0.0.1-SNAPSHOT.jar .
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to copy JAR file!${NC}"
    exit 1
fi

# Step 3: Build Docker image
echo -e "${YELLOW}Building Docker image: ${FULL_IMAGE_NAME}${NC}"
docker build -t ${FULL_IMAGE_NAME} -t ${LATEST_IMAGE_NAME} -t ${SNAPSHOT_IMAGE_NAME} .
if [ $? -ne 0 ]; then
    echo -e "${RED}Docker build failed!${NC}"
    exit 1
fi

# Step 4: Login to Docker Hub
echo -e "${YELLOW}Logging into Docker Hub...${NC}"
docker login
if [ $? -ne 0 ]; then
    echo -e "${RED}Docker login failed!${NC}"
    exit 1
fi

# Step 5: Push to registry
echo -e "${YELLOW}Pushing image to registry...${NC}"
docker push ${FULL_IMAGE_NAME}
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to push versioned image!${NC}"
    exit 1
fi

docker push ${LATEST_IMAGE_NAME}
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to push latest image!${NC}"
    exit 1
fi

docker push ${SNAPSHOT_IMAGE_NAME}
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to push snapshot image!${NC}"
    exit 1
fi

# Step 6: Cleanup
echo -e "${YELLOW}Cleaning up...${NC}"
rm -f KafkaConsumer-1.0-SNAPSHOT.jar

echo -e "${GREEN}Successfully built and pushed Docker image!${NC}"
echo -e "${GREEN}Image: ${FULL_IMAGE_NAME}${NC}"
echo -e "${GREEN}Latest: ${LATEST_IMAGE_NAME}${NC}"
echo -e "${GREEN}Snapshot: ${SNAPSHOT_IMAGE_NAME}${NC}"

# Optional: Show image details
echo -e "${YELLOW}Docker images:${NC}"
docker images | grep ${IMAGE_NAME}
