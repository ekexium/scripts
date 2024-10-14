#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Function to display usage information
usage() {
    echo "Usage: $0 [-n NAME] [-v VERSION]"
    echo ""
    echo "Options:"
    echo "  -n, --name      Name of the cluster (default: bench-pdml)"
    echo "  -v, --version   Version to deploy (required)"
    echo "  -h, --help      Display this help message"
    exit 1
}

# Default values
NAME="pdml"
VERSION=""

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -n|--name)
            NAME="$2"
            shift 2
            ;;
        -v|--version)
            VERSION="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown parameter passed: $1"
            usage
            ;;
    esac
done

# Check if VERSION is provided
if [[ -z "$VERSION" ]]; then
    echo "Error: VERSION is required."
    usage
fi

# Export variables
export NAME
export VERSION

echo "Deploying cluster with the following configuration:"
echo "Name: $NAME"
echo "Version: $VERSION"
echo ""

# Deploy cluster
tiup cluster template \
    --local \
    --deploy-dir /data/"$NAME"/deploy \
    --data-dir /data/"$NAME"/data \
    --monitoring-servers bench-pdml-load \
    --grafana-servers bench-pdml-load \
    --pd-servers bench-pdml-pd \
    --tikv-servers bench-pdml-tikv-0,bench-pdml-tikv-1,bench-pdml-tikv-2 \
    --tidb-servers bench-pdml-tidb-0 > cluster.yaml

tiup cluster check --apply cluster.yaml
tiup cluster deploy "$NAME" "$VERSION" cluster.yaml -y

# Patch binaries (Add your patch commands here)
# Example:
# tiup cluster patch "$NAME" some-patch-command

# Start cluster
tiup cluster start "$NAME"

# Load data
export TARGET_DB="ycsb_1e8_non_clustered"
tiup br:"$VERSION" restore db \
    --checksum=false \
    --pd bench-pdml-pd:2379 \
    --db "$TARGET_DB" \
    --storage "gs://oltp-bench-dataset-us-east5-nearline/v8.1.0/$TARGET_DB"

echo "Cluster deployment and data loading completed successfully."