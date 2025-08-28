#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Function to display usage information
usage() {
    echo "Usage: $0 [-n NAME] [-v VERSION] [-p PD_PATCH] [-d TIDB_PATCH] [-k TIKV_PATCH] [--skip-patches]"
    echo ""
    echo "Deploy next-gen TiDB cluster with binary patching for GCP instances"
    echo "Automatically generates cluster config based on GCP instance naming pattern."
    echo ""
    echo "Options:"
    echo "  -n, --name        Name of the cluster (default: nextgen-s3)"
    echo "  -v, --version     TiDB version to deploy (default: v9.0.0)"
    echo "  -p, --pd-patch    Path to PD hotfix tarball (default: /tmp/pd-hotfix-linux-amd64.tar.gz)"
    echo "  -d, --tidb-patch  Path to TiDB hotfix tarball (default: /tmp/tidb-hotfix-linux-amd64.tar.gz)"
    echo "  -k, --tikv-patch  Path to TiKV hotfix tarball (default: /tmp/tikv-hotfix-linux-amd64.tar.gz)"
    echo "  --skip-patches    Skip binary patching step"
    echo "  --skip-start      Deploy and patch only, don't start cluster"
    echo "  -h, --help        Display this help message"
    echo ""
    echo "Examples:"
    echo "  $0 -n test-nextgen -v v9.0.0"
    echo "  $0 -n my-cluster --skip-patches"
    echo "  $0 -n test --pd-patch /custom/path/pd.tar.gz"
    exit 1
}

# Function to check if file exists
check_file_exists() {
    local file=$1
    local description=$2

    if [[ ! -f "$file" ]]; then
        echo "Error: $description not found at: $file"
        echo "Please ensure the file exists or use appropriate flag to specify location"
        return 1
    fi
    return 0
}

# Function to generate next-gen cluster config
generate_nextgen_config() {
    echo "=== Generating Next-Gen Cluster Configuration ==="

    # Define server names based on cluster name (following GCP naming pattern)
    local MONITORING_SERVER="${CLUSTER_NAME}-load"
    local GRAFANA_SERVER="${CLUSTER_NAME}-load"
    local PD_SERVER="${CLUSTER_NAME}-pd-0"
    local TIKV_SERVERS="${CLUSTER_NAME}-tikv-0,${CLUSTER_NAME}-tikv-1,${CLUSTER_NAME}-tikv-2"
    local TIDB_SERVER="${CLUSTER_NAME}-tidb-0"
    local MINIO_SERVER="${CLUSTER_NAME}-minio"

    echo "Generating cluster topology..."
    echo "PD Server: $PD_SERVER"
    echo "TiKV Servers: $TIKV_SERVERS"
    echo "TiDB Server: $TIDB_SERVER"
    echo "MinIO Server: $MINIO_SERVER"

    # Generate base config with tiup cluster template
    tiup cluster template \
        --local \
        --deploy-dir /data/"$CLUSTER_NAME"/deploy \
        --data-dir /data/"$CLUSTER_NAME"/data \
        --monitoring-servers "$MONITORING_SERVER" \
        --grafana-servers "$GRAFANA_SERVER" \
        --pd-servers "$PD_SERVER" \
        --tikv-servers "$TIKV_SERVERS" \
        --tidb-servers "$TIDB_SERVER" > /tmp/cluster-base.yaml

    # Add next-gen specific configurations
    cat > "$CONFIG_FILE" << EOF
# Next-Gen TiDB Cluster Configuration
# Generated automatically for cluster: $CLUSTER_NAME

global:
  user: "tidb"
  ssh_port: 22
  deploy_dir: "/data/$CLUSTER_NAME/deploy"
  data_dir: "/data/$CLUSTER_NAME/data"
  arch: "amd64"

server_configs:
  tikv:
    storage.api-version: 2
    storage.enable-ttl: true
    dfs.prefix: "tikv"
    dfs.s3-endpoint: "http://$MINIO_SERVER:9000"
    dfs.s3-key-id: "minioadmin"
    dfs.s3-secret-key: "minioadmin"
    dfs.s3-bucket: "cse-test"
    dfs.s3-region: "local"
  pd:
    replication.location-labels: ["zone", "host"]
    keyspace.pre-alloc: ["cse-dedicated"]
  tidb:
    keyspace-name: "SYSTEM"

pd_servers:
  - host: $PD_SERVER

tidb_servers:
  - host: $TIDB_SERVER

tikv_servers:
$(echo "$TIKV_SERVERS" | tr ',' '\n' | while read -r tikv_host; do
    echo "  - host: $tikv_host"
    echo "    port: 20160"
    echo "    status_port: 20180"
done)

monitoring_servers:
  - host: $MONITORING_SERVER

grafana_servers:
  - host: $GRAFANA_SERVER
EOF

    echo "✓ Next-Gen cluster config generated: $CONFIG_FILE"
}

# Function to verify MinIO is accessible (simple check)
check_minio_status() {
    local minio_endpoint=$(grep "s3-endpoint" "$CONFIG_FILE" | cut -d'"' -f2 | head -1)

    if [[ -z "$minio_endpoint" ]]; then
        echo "No MinIO endpoint configured - skipping check"
        return 0
    fi

    echo "MinIO endpoint: $minio_endpoint"
    if curl -s --max-time 5 "$minio_endpoint" > /dev/null; then
        echo "✓ MinIO is accessible"
    else
        echo "⚠ MinIO may not be accessible (TiKV will handle bucket creation automatically)"
    fi
}

# Default values
CLUSTER_NAME="nextgen-s3"
VERSION="v9.0.0"
CONFIG_FILE="/tmp/nextgen-cluster.yaml"
PD_PATCH="/tmp/pd-hotfix-linux-amd64.tar.gz"
TIDB_PATCH="/tmp/tidb-hotfix-linux-amd64.tar.gz"
TIKV_PATCH="/tmp/tikv-hotfix-linux-amd64.tar.gz"
SKIP_PATCHES=false
SKIP_START=false

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -n|--name)
            CLUSTER_NAME="$2"
            shift 2
            ;;
        -v|--version)
            VERSION="$2"
            shift 2
            ;;
        -p|--pd-patch)
            PD_PATCH="$2"
            shift 2
            ;;
        -d|--tidb-patch)
            TIDB_PATCH="$2"
            shift 2
            ;;
        -k|--tikv-patch)
            TIKV_PATCH="$2"
            shift 2
            ;;
        --skip-patches)
            SKIP_PATCHES=true
            shift
            ;;
        --skip-start)
            SKIP_START=true
            shift
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

echo "========================================"
echo "Next-Gen TiDB Cluster Deployment Script"
echo "========================================"
echo "Cluster Name: $CLUSTER_NAME"
echo "TiDB Version: $VERSION"
echo "Config File:  $CONFIG_FILE"
echo "Skip Patches: $SKIP_PATCHES"
echo "Skip Start:   $SKIP_START"
echo "========================================"

# Generate cluster configuration (this replaces the need for a static config file)
generate_nextgen_config

# Validate patch files exist (if not skipping patches)
if [[ "$SKIP_PATCHES" == false ]]; then
    echo "=== Checking Patch Files ==="
    check_file_exists "$PD_PATCH" "PD hotfix" || exit 1
    check_file_exists "$TIDB_PATCH" "TiDB hotfix" || exit 1
    check_file_exists "$TIKV_PATCH" "TiKV hotfix" || exit 1
    echo "✓ All patch files found"
fi

# Check if cluster already exists
if tiup cluster list | grep -q "^$CLUSTER_NAME"; then
    echo "Warning: Cluster '$CLUSTER_NAME' already exists"
    read -p "Do you want to destroy and recreate it? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Destroying existing cluster..."
        tiup cluster destroy "$CLUSTER_NAME" --yes || true
        sleep 5
    else
        echo "Deployment cancelled"
        exit 1
    fi
fi

# Check if cluster already exists (moved here after config generation)

# Step 1: Deploy the cluster
echo ""
echo "=== Step 1: Deploying TiDB Cluster ==="
echo "Command: tiup cluster deploy $CLUSTER_NAME $VERSION $CONFIG_FILE --ignore-config-check"
tiup cluster deploy "$CLUSTER_NAME" "$VERSION" "$CONFIG_FILE" --ignore-config-check

# Step 2: Patch binaries (if not skipping)
if [[ "$SKIP_PATCHES" == false ]]; then
    echo ""
    echo "=== Step 2: Patching Binaries ==="

    echo "Patching PD..."
    echo "Command: tiup cluster patch $CLUSTER_NAME $PD_PATCH -R pd --offline -y"
    tiup cluster patch "$CLUSTER_NAME" "$PD_PATCH" -R pd --offline -y

    echo "Patching TiDB..."
    echo "Command: tiup cluster patch $CLUSTER_NAME $TIDB_PATCH -R tidb --offline -y"
    tiup cluster patch "$CLUSTER_NAME" "$TIDB_PATCH" -R tidb --offline -y

    echo "Patching TiKV..."
    echo "Command: tiup cluster patch $CLUSTER_NAME $TIKV_PATCH -R tikv --offline -y"
    tiup cluster patch "$CLUSTER_NAME" "$TIKV_PATCH" -R tikv --offline -y

    echo "✓ All binaries patched successfully"
else
    echo ""
    echo "=== Step 2: Skipping Binary Patching ==="
fi

# Step 3: Start the cluster (if not skipping)
if [[ "$SKIP_START" == false ]]; then
    echo ""
    echo "=== Step 3: Starting Cluster ==="
    echo "Command: tiup cluster start $CLUSTER_NAME"
    tiup cluster start "$CLUSTER_NAME"

    echo ""
    echo "=== Cluster Status ==="
    tiup cluster display "$CLUSTER_NAME"

    # Check MinIO status (optional)
    check_minio_status

    echo ""
    echo "🎉 Next-Gen TiDB cluster '$CLUSTER_NAME' deployed and started successfully!"
    echo ""
    echo "Next steps:"
    echo "1. Verify cluster status: tiup cluster display $CLUSTER_NAME"
    echo "2. Connect to TiDB: mysql -h <tidb-host> -P 4000 -u root"
    echo "3. Check MinIO console: http://<minio-host>:9001"
    echo "4. Run your next-gen TiDB tests!"
else
    echo ""
    echo "=== Step 3: Skipping Cluster Start ==="
    echo ""
    echo "🎉 Next-Gen TiDB cluster '$CLUSTER_NAME' deployed successfully!"
    echo ""
    echo "To start the cluster manually:"
    echo "  tiup cluster start $CLUSTER_NAME"
fi
