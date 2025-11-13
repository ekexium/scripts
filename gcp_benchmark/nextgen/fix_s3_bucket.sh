#!/bin/bash
#
# Quick fix script to create S3 bucket in MinIO
# Use this if the bucket wasn't created during deployment
#

set -e

CLUSTER_NAME="${1:-nextgen-s3}"
MINIO_HOST="${CLUSTER_NAME}-minio"
BUCKET_NAME="cse-test"

echo "========================================"
echo "S3 Bucket Creation Fix"
echo "========================================"
echo "Cluster:  $CLUSTER_NAME"
echo "MinIO:    $MINIO_HOST"
echo "Bucket:   $BUCKET_NAME"
echo "========================================"

# Check if MinIO is accessible
echo "Checking MinIO accessibility..."
if ! ssh -o StrictHostKeyChecking=no "$MINIO_HOST" "curl -s --max-time 5 http://localhost:9000 > /dev/null 2>&1"; then
    echo "Error: MinIO is not accessible on $MINIO_HOST"
    exit 1
fi
echo "✓ MinIO is accessible"

# Install mc and create bucket
echo "Creating S3 bucket..."
ssh -o StrictHostKeyChecking=no "$MINIO_HOST" "
    # Install MinIO client (mc) if not present
    if ! command -v mc &> /dev/null; then
        echo 'Installing MinIO client (mc)...'
        sudo wget -q https://dl.min.io/client/mc/release/linux-amd64/mc -O /usr/local/bin/mc
        sudo chmod +x /usr/local/bin/mc
        echo '✓ mc installed'
    else
        echo '✓ mc already installed'
    fi

    # Configure MinIO connection
    mc alias set myminio http://localhost:9000 minioadmin minioadmin &> /dev/null

    # Create bucket if it doesn't exist
    if mc ls myminio/$BUCKET_NAME &> /dev/null; then
        echo '✓ Bucket $BUCKET_NAME already exists'
    else
        echo 'Creating bucket $BUCKET_NAME...'
        mc mb myminio/$BUCKET_NAME
        if [ \$? -eq 0 ]; then
            echo '✓ Bucket $BUCKET_NAME created successfully'
        else
            echo '✗ Failed to create bucket $BUCKET_NAME'
            exit 1
        fi
    fi

    # List buckets to verify
    echo ''
    echo 'Current buckets in MinIO:'
    mc ls myminio/
"

echo ""
echo "========================================"
echo "✓ S3 Bucket Fix Completed"
echo "========================================"
echo ""
echo "The TiKV errors should stop now."
echo "If TiKV is still showing errors, wait 1-2 minutes for retry logic to work."
