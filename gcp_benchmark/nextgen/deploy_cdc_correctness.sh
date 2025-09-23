#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Function to display usage information
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Deploy CDC correctness testing environment with upstream and downstream TiDB clusters"
    echo ""
    echo "Options:"
    echo "  --upstream-version VERSION     TiDB version for upstream cluster (default: v9.0.0)"
    echo "  --downstream-version VERSION   TiDB version for downstream cluster (default: v9.0.0)"
    echo "  --pd-patch PATH                Path to PD hotfix tarball (default: /tmp/pd.tar.gz)"
    echo "  --tidb-patch PATH              Path to TiDB hotfix tarball (default: /tmp/tidb.tar.gz)"
    echo "  --tikv-patch PATH              Path to TiKV hotfix tarball (default: /tmp/tikv.tar.gz)"
    echo "  --cdc-patch PATH               Path to TiCDC hotfix tarball (default: /tmp/cdc.tar.gz)"
    echo "  --skip-patches                 Skip binary patching step"
    echo "  --cleanup                      Cleanup existing clusters before deployment"
    echo "  --skip-changefeed              Skip changefeed creation"
    echo "  -h, --help                     Display this help message"
    echo ""
    echo "Examples:"
    echo "  $0"
    echo "  $0 --upstream-version v8.5.0 --downstream-version v9.0.0"
    echo "  $0 --skip-patches"
    echo "  $0 --cleanup"
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

# Function to validate patch files
validate_patch_files() {
    if [[ "$SKIP_PATCHES" == true ]]; then
        echo "=== Skipping patch validation (--skip-patches specified) ==="
        return 0
    fi

    echo "=== Validating Next-Gen Patch Files ==="
    echo ""
    echo "⚠️  IMPORTANT: Next-gen TiDB requires custom binary patches!"
    echo "   Without patches, you'll get vanilla TiDB (missing S3/DFS features)"
    echo ""

    local missing_files=()

    if ! check_file_exists "$PD_PATCH" "PD hotfix"; then
        missing_files+=("PD patch: $PD_PATCH")
    fi
    if ! check_file_exists "$TIDB_PATCH" "TiDB hotfix"; then
        missing_files+=("TiDB patch: $TIDB_PATCH")
    fi
    if ! check_file_exists "$TIKV_PATCH" "TiKV hotfix"; then
        missing_files+=("TiKV patch: $TIKV_PATCH")
    fi
    if ! check_file_exists "$CDC_PATCH" "TiCDC hotfix"; then
        missing_files+=("TiCDC patch: $CDC_PATCH")
    fi

    if [ ${#missing_files[@]} -gt 0 ]; then
        echo ""
        echo "❌ Missing required patch files:"
        for file in "${missing_files[@]}"; do
            echo "   - $file"
        done
        echo ""
        echo "Options:"
        echo "1. Provide patch files at the expected locations"
        echo "2. Use --skip-patches to deploy vanilla TiDB (not recommended for next-gen testing)"
        echo "3. Use --pd-patch, --tidb-patch, --tikv-patch, --cdc-patch to specify custom paths"
        echo ""
        return 1
    fi

    echo "✓ All patch files found"
    echo "  PD:    $PD_PATCH"
    echo "  TiDB:  $TIDB_PATCH"
    echo "  TiKV:  $TIKV_PATCH"
    echo "  TiCDC: $CDC_PATCH"
    return 0
}

# Function to check if cluster exists
cluster_exists() {
    local cluster_name=$1
    tiup cluster list | grep -q "^$cluster_name" || return 1
}

# Function to wait for HTTP service to be ready
wait_for_http_service() {
    local host=$1
    local port=$2
    local service_name=$3
    local max_attempts=30
    local attempt=1

    echo "Waiting for $service_name to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if curl -s --connect-timeout 5 --max-time 5 "http://$host:$port" >/dev/null 2>&1; then
            echo "✓ $service_name is ready"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: $service_name not ready, waiting..."
        sleep 10
        attempt=$((attempt + 1))
    done

    echo "✗ $service_name failed to start after $max_attempts attempts"
    return 1
}

# Function to wait for MySQL service to be ready
wait_for_mysql_service() {
    local host=$1
    local port=$2
    local service_name=$3
    local max_attempts=30
    local attempt=1

    echo "Waiting for $service_name to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if mysqladmin ping -h "$host" -P "$port" -u root --connect-timeout=5 >/dev/null 2>&1; then
            echo "✓ $service_name is ready"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: $service_name not ready, waiting..."
        sleep 10
        attempt=$((attempt + 1))
    done

    echo "✗ $service_name failed to start after $max_attempts attempts"
    return 1
}

# Function to create MinIO buckets
setup_minio_buckets() {
    echo "=== Setting up MinIO buckets ==="

    # Wait for MinIO to be ready
    wait_for_http_service "ticdc-correctness-minio" 9000 "MinIO"

    # Install mc (MinIO client) if not present
    if ! command -v mc &> /dev/null; then
        echo "Installing MinIO client..."
        mkdir -p ~/.local/bin
        curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o ~/.local/bin/mc
        chmod +x ~/.local/bin/mc
        export PATH="$HOME/.local/bin:$PATH"
    fi

    # Configure MinIO alias
    mc alias set local http://ticdc-correctness-minio:9000 minioadmin minioadmin

    # Create buckets for upstream and downstream
    mc mb local/cse-upstream --ignore-existing
    mc mb local/cse-downstream --ignore-existing

    echo "✓ MinIO buckets created: cse-upstream, cse-downstream"
}

# Function to generate cluster config
generate_cluster_config() {
    local cluster_type=$1  # "upstream" or "downstream"
    local config_file=$2

    if [ "$cluster_type" = "upstream" ]; then
        local pd_host="ticdc-correctness-up-pd-0"
        local tikv_host="ticdc-correctness-up-tikv-0"
        local tidb_host="ticdc-correctness-up-tidb-0"
        local cdc_host="ticdc-correctness-cdc-0"
        local s3_bucket="cse-upstream"
        local monitoring_host="ticdc-correctness-load"
    else
        local pd_host="ticdc-correctness-down-pd-0"
        local tikv_host="ticdc-correctness-down-tikv-0"
        local tidb_host="ticdc-correctness-down-tidb-0"
        local s3_bucket="cse-downstream"
        local monitoring_host="ticdc-correctness-monitoring"
    fi

    cat > "$config_file" << EOF
global:
  user: "tidb"
  ssh_port: 22
  deploy_dir: "/data/tidb-deploy"
  data_dir: "/data/tidb-data"
  arch: "amd64"

server_configs:
  tikv:
    storage.api-version: 2
    storage.enable-ttl: true
    dfs.prefix: "tikv"
    dfs.s3-endpoint: "http://ticdc-correctness-minio:9000"
    dfs.s3-key-id: "minioadmin"
    dfs.s3-secret-key: "minioadmin"
    dfs.s3-bucket: "$s3_bucket"
    dfs.s3-region: "local"
  pd:
    replication.location-labels: ["zone", "host"]
    keyspace.pre-alloc: ["cse-dedicated"]
  tidb:
    keyspace-name: "SYSTEM"
    tidb.gc-life-time: "864000s"

pd_servers:
  - host: $pd_host

tidb_servers:
  - host: $tidb_host

tikv_servers:
  - host: $tikv_host
    port: 20160
    status_port: 20180

monitoring_servers:
  - host: $monitoring_host

grafana_servers:
  - host: $monitoring_host

EOF

    # Add CDC server only for upstream cluster
    if [ "$cluster_type" = "upstream" ]; then
        cat >> "$config_file" << EOF
cdc_servers:
  - host: $cdc_host
    port: 8300
    config:
      gc-ttl: 864000
      newarch: true

EOF
    fi

    echo "✓ Generated $cluster_type cluster config: $config_file"
}

# Function to patch cluster binaries
patch_cluster() {
    local cluster_name=$1
    local cluster_type=$2

    if [[ "$SKIP_PATCHES" == true ]]; then
        echo "=== Skipping binary patching for $cluster_type cluster ==="
        return 0
    fi

    echo "=== Patching $cluster_type cluster: $cluster_name ==="

    echo "Patching PD..."
    tiup cluster patch "$cluster_name" "$PD_PATCH" -R pd --offline -y

    echo "Patching TiDB..."
    tiup cluster patch "$cluster_name" "$TIDB_PATCH" -R tidb --offline -y

    echo "Patching TiKV..."
    tiup cluster patch "$cluster_name" "$TIKV_PATCH" -R tikv --offline -y

    # Patch TiCDC only for upstream cluster (has CDC server)
    if [ "$cluster_type" = "upstream" ]; then
        echo "Patching TiCDC..."
        tiup cluster patch "$cluster_name" "$CDC_PATCH" -R cdc --offline -y
    fi

    echo "✓ All binaries patched successfully for $cluster_type cluster"
}

# Function to deploy cluster
deploy_cluster() {
    local cluster_name=$1
    local cluster_type=$2
    local version=$3
    local config_file="/tmp/${cluster_name}-config.yaml"

    echo "=== Deploying $cluster_type cluster: $cluster_name ==="

    # Generate cluster config
    generate_cluster_config "$cluster_type" "$config_file"

    # Check if cluster already exists
    if cluster_exists "$cluster_name"; then
        echo "Warning: Cluster '$cluster_name' already exists"
        if [ "$CLEANUP" = true ]; then
            echo "Destroying existing cluster..."
            tiup cluster destroy "$cluster_name" --yes || true
            sleep 5
        else
            echo "Use --cleanup to remove existing cluster"
            return 1
        fi
    fi

    # Deploy the cluster (offline)
    echo "Deploying cluster with version $version..."
    tiup cluster deploy "$cluster_name" "$version" "$config_file" --ignore-config-check

    # Patch binaries (before starting)
    patch_cluster "$cluster_name" "$cluster_type"

    # Start the cluster
    echo "Starting cluster..."
    tiup cluster start "$cluster_name"

    # Verify cluster status
    echo "Cluster status:"
    tiup cluster display "$cluster_name"

    echo "✓ $cluster_type cluster deployed and started successfully"
}

# Function to create changefeed
create_changefeed() {
    echo "=== Creating CDC changefeed ==="

    # Wait for CDC server to be ready
    wait_for_http_service "ticdc-correctness-cdc-0" 8300 "TiCDC"

    # Wait for downstream TiDB to be ready
    wait_for_mysql_service "ticdc-correctness-down-tidb-0" 4000 "Downstream TiDB"

    # Create changefeed
    echo "Creating changefeed: upstream → downstream"
    tiup cdc cli changefeed create \
        --server="http://ticdc-correctness-cdc-0:8300" \
        --changefeed-id="cdc-correctness-basic" \
        --sink-uri="mysql://root@ticdc-correctness-down-tidb-0:4000/"

    # Verify changefeed status
    echo "Changefeed status:"
    tiup cdc cli changefeed list --server="http://ticdc-correctness-cdc-0:8300"

    echo "✓ Changefeed created successfully"
}

# Default values
UPSTREAM_VERSION="v9.0.0"
DOWNSTREAM_VERSION="v9.0.0"
PD_PATCH="/tmp/pd.tar.gz"
TIDB_PATCH="/tmp/tidb.tar.gz"
TIKV_PATCH="/tmp/tikv.tar.gz"
CDC_PATCH="/tmp/cdc.tar.gz"
SKIP_PATCHES=false
CLEANUP=false
SKIP_CHANGEFEED=false

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --upstream-version)
            UPSTREAM_VERSION="$2"
            shift 2
            ;;
        --downstream-version)
            DOWNSTREAM_VERSION="$2"
            shift 2
            ;;
        --pd-patch)
            PD_PATCH="$2"
            shift 2
            ;;
        --tidb-patch)
            TIDB_PATCH="$2"
            shift 2
            ;;
        --tikv-patch)
            TIKV_PATCH="$2"
            shift 2
            ;;
        --cdc-patch)
            CDC_PATCH="$2"
            shift 2
            ;;
        --skip-patches)
            SKIP_PATCHES=true
            shift
            ;;
        --cleanup)
            CLEANUP=true
            shift
            ;;
        --skip-changefeed)
            SKIP_CHANGEFEED=true
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
echo "CDC Correctness Testing Deployment"
echo "========================================"
echo "Upstream Version:   $UPSTREAM_VERSION"
echo "Downstream Version: $DOWNSTREAM_VERSION"
echo "Skip Patches:       $SKIP_PATCHES"
echo "Cleanup:            $CLEANUP"
echo "Skip Changefeed:    $SKIP_CHANGEFEED"
echo "========================================"

# Step 1: Validate patch files
validate_patch_files

# Step 2: Setup MinIO buckets
setup_minio_buckets

# Step 3: Deploy upstream cluster
deploy_cluster "cdc-correctness-upstream" "upstream" "$UPSTREAM_VERSION"

# Step 4: Deploy downstream cluster
deploy_cluster "cdc-correctness-downstream" "downstream" "$DOWNSTREAM_VERSION"

# Step 5: Create changefeed (if not skipping)
if [ "$SKIP_CHANGEFEED" = false ]; then
    create_changefeed
else
    echo "=== Skipping changefeed creation ==="
fi

echo ""
echo "🎉 CDC Correctness Environment Ready!"
echo ""
echo "Clusters:"
echo "  Upstream:   cdc-correctness-upstream"
echo "  Downstream: cdc-correctness-downstream"
echo ""
echo "Connection Info:"
echo "  Upstream TiDB:   ticdc-correctness-up-tidb-0:4000"
echo "  Downstream TiDB: ticdc-correctness-down-tidb-0:4000"
echo "  TiCDC Server:    ticdc-correctness-cdc-0:8300"
echo "  MinIO Console:   ticdc-correctness-minio:9001"
echo ""
echo "Monitoring (Separate VMs):"
echo "  Upstream Grafana:    http://ticdc-correctness-load:3000"
echo "  Upstream Prometheus: http://ticdc-correctness-load:9090"
echo "  Downstream Grafana:  http://ticdc-correctness-monitoring:3000"
echo "  Downstream Prometheus: http://ticdc-correctness-monitoring:9090"
echo ""
echo "Storage Isolation:"
echo "  Upstream S3:     cse-upstream bucket"
echo "  Downstream S3:   cse-downstream bucket"
echo ""
if [ "$SKIP_PATCHES" = false ]; then
echo "Next-Gen Patches Applied:"
echo "  PD:    $(basename "$PD_PATCH")"
echo "  TiDB:  $(basename "$TIDB_PATCH")"
echo "  TiKV:  $(basename "$TIKV_PATCH")"
echo "  TiCDC: $(basename "$CDC_PATCH")"
else
echo "⚠️  Patches: SKIPPED (vanilla TiDB deployed)"
fi
echo ""
if [ "$SKIP_CHANGEFEED" = false ]; then
echo "Changefeed:"
echo "  ID: cdc-correctness-basic"
echo "  Status: Check with 'tiup cdc cli changefeed list --server=http://ticdc-correctness-cdc-0:8300'"
echo ""
fi
echo "Ready for your custom workload testing!"