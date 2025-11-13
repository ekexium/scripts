#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Function to display usage information
usage() {
    echo "Usage: $0 [-n NAME] [-v VERSION] [-p PD_PATCH] [-d TIDB_PATCH] [-k TIKV_PATCH] [-w TIKV_WORKER_PATCH] [--skip-patches]"
    echo ""
    echo "Deploy TiDB-X (Next-Gen) cluster with binary patching for GCP instances"
    echo "Automatically generates cluster config based on GCP instance naming pattern."
    echo ""
    echo "Options:"
    echo "  -n, --name              Name of the cluster (default: nextgen-s3)"
    echo "  -v, --version           TiDB version to deploy (default: v8.5.0)"
    echo "  -p, --pd-patch          Path to PD tarball (default: /tmp/pd.tar.gz)"
    echo "  -d, --tidb-patch        Path to TiDB tarball (default: /tmp/tidb.tar.gz)"
    echo "  -k, --tikv-patch        Path to TiKV tarball (default: /tmp/tikv.tar.gz)"
    echo "  -w, --tikv-worker-patch Path to TiKV Worker tarball (default: /tmp/tikv-worker.tar.gz)"
    echo "  --skip-patches          Skip binary patching step"
    echo "  --skip-start            Deploy and patch only, don't start cluster"
    echo "  -h, --help              Display this help message"
    echo ""
    echo "Examples:"
    echo "  $0 -n test-nextgen -v v8.5.0"
    echo "  $0 -n my-cluster --skip-patches"
    echo "  $0 -n test --pd-patch /custom/path/pd.tar.gz --tikv-worker-patch /custom/path/tikv-worker.tar.gz"
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
    local SYSTEM_TIDB_SERVER="${CLUSTER_NAME}-tidb-system"
    local TIDB_SERVER="${CLUSTER_NAME}-tidb-0"
    local TIKV_WORKER_SERVER="${CLUSTER_NAME}-tikv-worker"
    local MINIO_SERVER="${CLUSTER_NAME}-minio"

    echo "Generating cluster topology..."
    echo "PD Server: $PD_SERVER"
    echo "TiKV Servers: $TIKV_SERVERS"
    echo "System TiDB Server: $SYSTEM_TIDB_SERVER (port 3000)"
    echo "User TiDB Server: $TIDB_SERVER (port 4000)"
    echo "TiKV Worker Server: $TIKV_WORKER_SERVER (port 19000)"
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

    # Add next-gen specific configurations for TiDB-X mode
    cat > "$CONFIG_FILE" << EOF
# TiDB-X (Next-Gen) Cluster Configuration
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
    storage.use-separated-scheduler-pool: false
    dfs.prefix: "tikv"
    dfs.s3-endpoint: "http://$MINIO_SERVER:9000"
    dfs.s3-key-id: "minioadmin"
    dfs.s3-secret-key: "minioadmin"
    dfs.s3-bucket: "cse-test"
    dfs.s3-region: "local"
  pd:
    replication.location-labels: ["zone", "host"]
    keyspace.pre-alloc: ["SYSTEM", "keyspace1"]
  tidb:
    security.auto-tls: true

pd_servers:
  - host: $PD_SERVER

tidb_servers:
  # System TiDB - MUST start first with 15s wait before User TiDB
  - host: $SYSTEM_TIDB_SERVER
    port: 3000
    status_port: 10080
    config:
      instance.tidb_service_scope: "dxf_service"
      tikv-worker-url: "http://$TIKV_WORKER_SERVER:19000"
      keyspace-name: "SYSTEM"
      enable-safe-point-v2: true
      split-table: false
      use-autoscaler: false
      disaggregated-tiflash: false

  # User TiDB - Starts AFTER System TiDB (15s delay)
  - host: $TIDB_SERVER
    port: 4000
    status_port: 10081
    config:
      keyspace-name: "keyspace1"
      enable-safe-point-v2: true
      split-table: false
      use-autoscaler: false
      disaggregated-tiflash: false

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

# Function to create S3 bucket in MinIO
create_s3_bucket() {
    echo "=== Setting up S3 Bucket ==="

    local MINIO_HOST="${CLUSTER_NAME}-minio"
    local BUCKET_NAME="cse-test"
    local CURRENT_USER=$(whoami)

    echo "Creating S3 bucket '$BUCKET_NAME' on $MINIO_HOST..."

    # Check if MinIO is accessible
    if ! ssh -o StrictHostKeyChecking=no "$CURRENT_USER@$MINIO_HOST" "curl -s --max-time 5 http://localhost:9000 > /dev/null 2>&1"; then
        echo "⚠ Warning: MinIO may not be running on $MINIO_HOST"
        echo "  Continuing anyway - bucket creation will be skipped"
        return 0
    fi

    # Install mc and create bucket
    ssh -o StrictHostKeyChecking=no "$CURRENT_USER@$MINIO_HOST" "
        # Install MinIO client (mc) if not present
        if ! command -v mc &> /dev/null; then
            echo 'Installing MinIO client (mc)...'
            sudo wget -q https://dl.min.io/client/mc/release/linux-amd64/mc -O /usr/local/bin/mc
            sudo chmod +x /usr/local/bin/mc
        fi

        # Configure MinIO connection
        mc alias set myminio http://localhost:9000 minioadmin minioadmin &> /dev/null || true

        # Create bucket if it doesn't exist
        if mc ls myminio/$BUCKET_NAME &> /dev/null; then
            echo '✓ Bucket $BUCKET_NAME already exists'
        else
            mc mb myminio/$BUCKET_NAME
            if [ \$? -eq 0 ]; then
                echo '✓ Bucket $BUCKET_NAME created successfully'
            else
                echo '✗ Failed to create bucket $BUCKET_NAME'
                return 1
            fi
        fi

        # Verify bucket exists
        mc ls myminio/ | grep $BUCKET_NAME
    " || {
        echo "⚠ Warning: Failed to create S3 bucket, but continuing..."
        return 0
    }

    echo "✓ S3 bucket setup completed"
    return 0
}

# Function to extract tikv-worker binary from separate tarball
extract_tikv_worker() {
    echo "=== Extracting TiKV Worker Binary ==="

    if [[ ! -f "$TIKV_WORKER_PATCH" ]]; then
        echo "Error: TiKV Worker tarball not found: $TIKV_WORKER_PATCH"
        return 1
    fi

    local extract_dir="/tmp/tikv-worker-extract-$$"
    mkdir -p "$extract_dir"

    echo "Extracting TiKV Worker tarball..."
    tar -xzf "$TIKV_WORKER_PATCH" -C "$extract_dir"

    # Find tikv-worker binary in extracted directory
    local tikv_worker_bin=$(find "$extract_dir" -name "tikv-worker" -type f | head -1)

    if [[ -z "$tikv_worker_bin" ]]; then
        echo "Error: tikv-worker binary not found in tarball"
        rm -rf "$extract_dir"
        return 1
    fi

    echo "✓ Found tikv-worker at: $tikv_worker_bin"
    cp "$tikv_worker_bin" /tmp/tikv-worker
    chmod +x /tmp/tikv-worker
    rm -rf "$extract_dir"

    echo "✓ TiKV Worker binary extracted to /tmp/tikv-worker"
    return 0
}

# Function to deploy and start TiKV Worker
deploy_tikv_worker() {
    echo "=== Deploying TiKV Worker ==="

    local TIKV_WORKER_HOST="${CLUSTER_NAME}-tikv-worker"
    local MINIO_HOST="${CLUSTER_NAME}-minio"
    local PD_HOST="${CLUSTER_NAME}-pd-0"

    # Detect the current user
    local CURRENT_USER=$(whoami)
    echo "Deploying as user: $CURRENT_USER"

    # Create tidb user on TiKV Worker VM if it doesn't exist
    echo "Setting up tidb user on $TIKV_WORKER_HOST..."
    ssh -o StrictHostKeyChecking=no "$CURRENT_USER@$TIKV_WORKER_HOST" "
        if ! id tidb &>/dev/null; then
            echo 'Creating tidb user...'
            sudo useradd -m -s /bin/bash tidb
            sudo mkdir -p /home/tidb/.ssh
            sudo cp ~/.ssh/authorized_keys /home/tidb/.ssh/
            sudo chown -R tidb:tidb /home/tidb/.ssh
            sudo chmod 700 /home/tidb/.ssh
            sudo chmod 600 /home/tidb/.ssh/authorized_keys
        fi
        sudo mkdir -p /data/tikv-worker/{bin,conf,logs,schemas}
        sudo chown -R tidb:tidb /data/tikv-worker
    "

    # Copy tikv-worker binary
    echo "Copying tikv-worker binary to $TIKV_WORKER_HOST..."
    scp -o StrictHostKeyChecking=no /tmp/tikv-worker "tidb@$TIKV_WORKER_HOST:/data/tikv-worker/bin/"

    # Generate TiKV Worker config
    echo "Generating TiKV Worker configuration..."
    cat > /tmp/tikv_worker.toml << TIKV_WORKER_CONFIG
[dfs]
prefix = "tikv"
s3-endpoint = "http://$MINIO_HOST:9000"
s3-key-id = "minioadmin"
s3-secret-key = "minioadmin"
s3-bucket = "cse-test"
s3-region = "local"

[raft-engine]
enabled = false

[schema-manager]
dir = "/data/tikv-worker/schemas"
schema-refresh-threshold = 1
enabled = true
keyspace-refresh-interval = "10s"
TIKV_WORKER_CONFIG

    # Copy config to remote host
    scp -o StrictHostKeyChecking=no /tmp/tikv_worker.toml "tidb@$TIKV_WORKER_HOST:/data/tikv-worker/conf/"

    # Create start script
    cat > /tmp/start_tikv_worker.sh << 'START_SCRIPT'
#!/bin/bash
cd /data/tikv-worker
nohup ./bin/tikv-worker \
  --addr=0.0.0.0:19000 \
  --pd-endpoints=http://PD_HOST_PLACEHOLDER:2379 \
  --config=conf/tikv_worker.toml \
  --log-file=logs/tikv_worker.log \
  > logs/stdout.log 2>&1 &
echo $! > tikv_worker.pid
echo "TiKV Worker started with PID: $(cat tikv_worker.pid)"
START_SCRIPT

    # Replace placeholder with actual PD host
    sed -i "s/PD_HOST_PLACEHOLDER/$PD_HOST/g" /tmp/start_tikv_worker.sh
    chmod +x /tmp/start_tikv_worker.sh

    # Copy start script to remote host
    scp -o StrictHostKeyChecking=no /tmp/start_tikv_worker.sh "tidb@$TIKV_WORKER_HOST:/data/tikv-worker/"

    # Start TiKV Worker
    echo "Starting TiKV Worker on $TIKV_WORKER_HOST..."
    ssh -o StrictHostKeyChecking=no "tidb@$TIKV_WORKER_HOST" "/data/tikv-worker/start_tikv_worker.sh"

    # Wait for TiKV Worker to be ready
    echo "Waiting for TiKV Worker to be ready..."
    local retries=30
    local wait_time=2
    for ((i=1; i<=retries; i++)); do
        if ssh -o StrictHostKeyChecking=no "tidb@$TIKV_WORKER_HOST" "curl -s http://localhost:19000/metrics > /dev/null 2>&1"; then
            echo "✓ TiKV Worker is ready"
            return 0
        fi
        echo "Waiting for TiKV Worker... ($i/$retries)"
        sleep $wait_time
    done

    echo "⚠ TiKV Worker may not be fully ready yet (continuing anyway)"
    return 0
}

# Function to start cluster with correct TiDB-X component order
start_cluster_tidb_x() {
    echo "=== Starting TiDB-X Cluster (Correct Component Order) ==="

    local SYSTEM_TIDB_HOST="${CLUSTER_NAME}-tidb-system"
    local USER_TIDB_HOST="${CLUSTER_NAME}-tidb-0"

    # Step 1: Start PD
    echo "Step 1: Starting PD..."
    tiup cluster start "$CLUSTER_NAME" -R pd
    echo "Waiting for PD to be ready..."
    sleep 5

    # Step 1.5: Create S3 bucket (before TiKV starts)
    echo "Step 1.5: Creating S3 bucket..."
    create_s3_bucket

    # Step 2: Start TiKV
    echo "Step 2: Starting TiKV..."
    tiup cluster start "$CLUSTER_NAME" -R tikv
    echo "Waiting for TiKV to be ready..."
    sleep 10

    # Step 3: Start TiKV Worker (manual deployment)
    echo "Step 3: Starting TiKV Worker..."
    deploy_tikv_worker

    # Step 4: Start System TiDB FIRST
    echo "Step 4: Starting System TiDB (port 3000)..."
    tiup cluster start "$CLUSTER_NAME" -R tidb -N "$SYSTEM_TIDB_HOST:3000"

    # CRITICAL: Wait 15 seconds for System TiDB initialization
    echo "⏱  Waiting 15 seconds for System TiDB initialization (CRITICAL)..."
    sleep 15

    # Step 5: Start User TiDB
    echo "Step 5: Starting User TiDB (port 4000)..."
    tiup cluster start "$CLUSTER_NAME" -R tidb -N "$USER_TIDB_HOST:4000"

    # Step 6: Start monitoring
    echo "Step 6: Starting monitoring and Grafana..."
    tiup cluster start "$CLUSTER_NAME" -R prometheus,grafana || true

    echo "✓ TiDB-X cluster started successfully"
}

# Default values
CLUSTER_NAME="nextgen-s3"
VERSION="v8.5.0"
CONFIG_FILE="/tmp/nextgen-cluster.yaml"
PD_PATCH="/tmp/pd.tar.gz"
TIDB_PATCH="/tmp/tidb.tar.gz"
TIKV_PATCH="/tmp/tikv.tar.gz"
TIKV_WORKER_PATCH="/tmp/tikv-worker.tar.gz"
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
        -w|--tikv-worker-patch)
            TIKV_WORKER_PATCH="$2"
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
echo "TiDB-X Cluster Deployment Script"
echo "========================================"
echo "Cluster Name:       $CLUSTER_NAME"
echo "TiDB Version:       $VERSION"
echo "Config File:        $CONFIG_FILE"
echo "PD Patch:           $PD_PATCH"
echo "TiDB Patch:         $TIDB_PATCH"
echo "TiKV Patch:         $TIKV_PATCH"
echo "TiKV Worker Patch:  $TIKV_WORKER_PATCH"
echo "Skip Patches:       $SKIP_PATCHES"
echo "Skip Start:         $SKIP_START"
echo "========================================"

# Generate cluster configuration (this replaces the need for a static config file)
generate_nextgen_config

# Validate patch files exist (if not skipping patches)
if [[ "$SKIP_PATCHES" == false ]]; then
    echo "=== Checking Patch Files ==="
    check_file_exists "$PD_PATCH" "PD tarball" || exit 1
    check_file_exists "$TIDB_PATCH" "TiDB tarball" || exit 1
    check_file_exists "$TIKV_PATCH" "TiKV tarball" || exit 1
    check_file_exists "$TIKV_WORKER_PATCH" "TiKV Worker tarball" || exit 1
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

    # Extract TiKV Worker from TiKV tarball
    echo ""
    echo "Extracting TiKV Worker binary..."
    extract_tikv_worker || {
        echo "⚠ Warning: Failed to extract tikv-worker, but continuing..."
    }
else
    echo ""
    echo "=== Step 2: Skipping Binary Patching ==="
fi

# Step 3: Start the cluster (if not skipping)
if [[ "$SKIP_START" == false ]]; then
    echo ""
    echo "=== Step 3: Starting TiDB-X Cluster ==="

    # Use TiDB-X specific startup sequence (PD -> TiKV -> TiKV Worker -> System TiDB [15s] -> User TiDB)
    start_cluster_tidb_x

    echo ""
    echo "=== Cluster Status ==="
    tiup cluster display "$CLUSTER_NAME"

    # Check MinIO status (optional)
    check_minio_status

    echo ""
    echo "🎉 TiDB-X cluster '$CLUSTER_NAME' deployed and started successfully!"
    echo ""
    echo "Cluster Architecture:"
    echo "  - PD:             ${CLUSTER_NAME}-pd-0:2379"
    echo "  - TiKV:           ${CLUSTER_NAME}-tikv-{0,1,2}:20160"
    echo "  - TiKV Worker:    ${CLUSTER_NAME}-tikv-worker:19000"
    echo "  - System TiDB:    ${CLUSTER_NAME}-tidb-system:3000 (keyspace=SYSTEM)"
    echo "  - User TiDB:      ${CLUSTER_NAME}-tidb-0:4000 (keyspace=keyspace1)"
    echo "  - MinIO:          ${CLUSTER_NAME}-minio:9000"
    echo ""
    echo "Next steps:"
    echo "1. Verify cluster status: tiup cluster display $CLUSTER_NAME"
    echo "2. Connect to User TiDB: mysql -h ${CLUSTER_NAME}-tidb-0 -P 4000 -u root"
    echo "3. Check System TiDB: mysql -h ${CLUSTER_NAME}-tidb-system -P 3000 -u root"
    echo "4. Check MinIO console: http://${CLUSTER_NAME}-minio:9001 (admin/minioadmin)"
    echo "5. Run sysbench oltp_read_write tests!"
    echo ""
    echo "Testing with different TiKV configs:"
    echo "  Current config: storage.use_separated_scheduler_pool=false"
    echo "  To test with =true:"
    echo "    1. tiup cluster edit-config $CLUSTER_NAME"
    echo "    2. Change storage.use_separated_scheduler_pool to true"
    echo "    3. tiup cluster reload $CLUSTER_NAME -R tikv"
else
    echo ""
    echo "=== Step 3: Skipping Cluster Start ==="
    echo ""
    echo "🎉 TiDB-X cluster '$CLUSTER_NAME' deployed successfully!"
    echo ""
    echo "To start the cluster manually, you must follow TiDB-X component order:"
    echo "  1. Start PD:          tiup cluster start $CLUSTER_NAME -R pd"
    echo "  2. Start TiKV:        tiup cluster start $CLUSTER_NAME -R tikv"
    echo "  3. Deploy TiKV Worker manually on ${CLUSTER_NAME}-tikv-worker"
    echo "  4. Start System TiDB: tiup cluster start $CLUSTER_NAME -R tidb -N ${CLUSTER_NAME}-tidb-system:3000"
    echo "  5. WAIT 15 SECONDS"
    echo "  6. Start User TiDB:   tiup cluster start $CLUSTER_NAME -R tidb -N ${CLUSTER_NAME}-tidb-0:4000"
fi
