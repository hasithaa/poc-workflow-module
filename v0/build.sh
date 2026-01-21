#!/bin/bash

# Build script for Workflow V0 module and examples
# This script builds the native implementation and Ballerina modules in the correct order

set -e  # Exit on error

echo "========================================="
echo "Building Workflow V0 Module"
echo "========================================="

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Step 1: Build Native Implementation
echo -e "\n${BLUE}Step 1: Building Native Java Implementation...${NC}"
cd native
if [ -f "pom.xml" ]; then
    mvn clean package
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Native implementation built successfully${NC}"
    else
        echo -e "${RED}✗ Failed to build native implementation${NC}"
        exit 1
    fi
else
    echo -e "${RED}✗ pom.xml not found in native directory${NC}"
    exit 1
fi
cd ..

# Step 2: Build Workflow Module
echo -e "\n${BLUE}Step 2: Building Workflow Module (hasitha/workflow_v0)...${NC}"
cd ballerina_workflow
if [ -f "Ballerina.toml" ]; then
    # Clean before building to ensure fresh build
    echo -e "${BLUE}Cleaning previous build...${NC}"
    bal clean
    
    bal pack
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Workflow module built successfully${NC}"
        
        # Push to local repository
        echo -e "${BLUE}Pushing to local repository...${NC}"
        bal push --repository=local
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✓ Pushed to local repository${NC}"
        else
            echo -e "${RED}✗ Failed to push to local repository${NC}"
            exit 1
        fi
    else
        echo -e "${RED}✗ Failed to build workflow module${NC}"
        exit 1
    fi
else
    echo -e "${RED}✗ Ballerina.toml not found in ballerina_workflow directory${NC}"
    exit 1
fi
cd ..

# Step 3: Build Example
echo -e "\n${BLUE}Step 3: Building Example (simpleuser)...${NC}"
cd examples/simpleuser
if [ -f "Ballerina.toml" ]; then
    # Clean before building to ensure fresh build
    echo -e "${BLUE}Cleaning previous build...${NC}"
    bal clean
    
    bal build
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Example built successfully${NC}"
    else
        echo -e "${RED}✗ Failed to build example${NC}"
        exit 1
    fi
else
    echo -e "${RED}✗ Ballerina.toml not found in examples/simpleuser directory${NC}"
    exit 1
fi
cd ../..

# Success message
echo -e "\n${GREEN}========================================="
echo "✓ All V0 modules built successfully!"
echo "=========================================${NC}"
echo ""
echo "To run the example:"
echo "  cd examples/simpleuser"
echo "  bal run"
echo ""
echo "Note: Ensure Temporal server is running at localhost:7233"
