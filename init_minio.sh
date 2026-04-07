#!/bin/bash
# Wait for MinIO to be ready
until curl -f http://localhost:9000/minio/health/live 2>/dev/null; do
    echo "Waiting for MinIO..."
    sleep 1
done

# Create the datalake bucket
mc alias set local http://localhost:9000 admin admin12345
mc mb local/datalake --ignore-existing

echo "MinIO bucket 'datalake' created successfully"