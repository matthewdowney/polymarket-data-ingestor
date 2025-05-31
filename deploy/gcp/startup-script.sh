#!/bin/bash
apt update
apt install -y htop tmux jq google-cloud-ops-agent

# Create user and directories
useradd -m polymarket
mkdir -p /opt/polymarket-logger/{data,scripts}
mkdir -p /var/log/polymarket-logger
chown -R polymarket:polymarket /opt/polymarket-logger /var/log/polymarket-logger

# Set environment variables
cat >> /etc/environment << EOF
BUCKET_NAME=${bucket_name}
PROJECT_ID=${project_id}
REGION=${region}
EOF

# Create upload script
cat > /opt/polymarket-logger/scripts/upload.sh << "UPLOAD_EOF"
#!/bin/bash
set -e
source /etc/environment

DATA_DIR="/opt/polymarket-logger/data"
CURRENT_HOUR=$(date +%Y-%m-%d-%H)

cd "$DATA_DIR"
for file in *.jsonl.zst; do
    [ "$file" = "$CURRENT_HOUR.jsonl.zst" ] && continue
    [ ! -f "$file" ] && continue
    
    GCS_PATH="gs://$BUCKET_NAME/raw/$(date -r "$file" +%Y/%m/%d)/$file"
    if gsutil cp "$file" "$GCS_PATH"; then
        rm "$file"
        # Send custom metric to Cloud Monitoring
        gcloud logging write polymarket-upload "File uploaded: $file" --severity=INFO
    fi
done
UPLOAD_EOF

# Create health check script
cat > /opt/polymarket-logger/scripts/health.sh << "HEALTH_EOF"
#!/bin/bash
source /etc/environment

# Check service status
if systemctl is-active --quiet polymarket-logger; then
    SERVICE_RUNNING=1
else
    SERVICE_RUNNING=0
fi

# Send custom metrics to Cloud Monitoring
gcloud monitoring metrics create custom.googleapis.com/polymarket/service_running \
    --type=gauge --value-type=double --description="Service running status" 2>/dev/null || true

echo "{\"metricKind\": \"GAUGE\", \"valueType\": \"DOUBLE\", \"timeSeries\": [{\"resource\": {\"type\": \"gce_instance\", \"labels\": {\"instance_id\": \"$(curl -s http://metadata.google.internal/computeMetadata/v1/instance/id -H 'Metadata-Flavor: Google')\", \"zone\": \"$(curl -s http://metadata.google.internal/computeMetadata/v1/instance/zone -H 'Metadata-Flavor: Google' | cut -d/ -f4)\"}}, \"metric\": {\"type\": \"custom.googleapis.com/polymarket/service_running\"}, \"points\": [{\"interval\": {\"endTime\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}, \"value\": {\"doubleValue\": $SERVICE_RUNNING}}]}]}" | \
gcloud monitoring time-series create --data-from-stdin 2>/dev/null || true

# Parse logs for metrics
LATEST_METRICS=$(journalctl -u polymarket-logger --since "2 minutes ago" | grep "feed metrics" | tail -1)

if [ ! -z "$LATEST_METRICS" ]; then
    CONNECTIONS=$(echo "$LATEST_METRICS" | grep -o 'open_connections=[0-9]*' | cut -d= -f2)
    MESSAGES_SEC=$(echo "$LATEST_METRICS" | grep -o 'messages_per_sec=[0-9]*' | cut -d= -f2)
    BYTES_SEC=$(echo "$LATEST_METRICS" | grep -o 'bytes_per_sec=[0-9]*' | cut -d= -f2)
    
    # Create custom metrics if they don't exist
    gcloud monitoring metrics create custom.googleapis.com/polymarket/active_connections \
        --type=gauge --value-type=int64 --description="Active connections" 2>/dev/null || true
    gcloud monitoring metrics create custom.googleapis.com/polymarket/messages_per_sec \
        --type=gauge --value-type=int64 --description="Messages per second" 2>/dev/null || true
    gcloud monitoring metrics create custom.googleapis.com/polymarket/bytes_per_sec \
        --type=gauge --value-type=int64 --description="Bytes per second" 2>/dev/null || true
    
    # Send metrics
    INSTANCE_ID=$(curl -s http://metadata.google.internal/computeMetadata/v1/instance/id -H 'Metadata-Flavor: Google')
    ZONE=$(curl -s http://metadata.google.internal/computeMetadata/v1/instance/zone -H 'Metadata-Flavor: Google' | cut -d/ -f4)
    TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    
    # Send active connections metric
    echo "{\"metricKind\": \"GAUGE\", \"valueType\": \"INT64\", \"timeSeries\": [{\"resource\": {\"type\": \"gce_instance\", \"labels\": {\"instance_id\": \"$INSTANCE_ID\", \"zone\": \"$ZONE\"}}, \"metric\": {\"type\": \"custom.googleapis.com/polymarket/active_connections\"}, \"points\": [{\"interval\": {\"endTime\": \"$TIMESTAMP\"}, \"value\": {\"int64Value\": \"$CONNECTIONS\"}}]}]}" | \
    gcloud monitoring time-series create --data-from-stdin 2>/dev/null || true

    # Send messages per second metric
    echo "{\"metricKind\": \"GAUGE\", \"valueType\": \"INT64\", \"timeSeries\": [{\"resource\": {\"type\": \"gce_instance\", \"labels\": {\"instance_id\": \"$INSTANCE_ID\", \"zone\": \"$ZONE\"}}, \"metric\": {\"type\": \"custom.googleapis.com/polymarket/messages_per_sec\"}, \"points\": [{\"interval\": {\"endTime\": \"$TIMESTAMP\"}, \"value\": {\"int64Value\": \"$MESSAGES_SEC\"}}]}]}" | \
    gcloud monitoring time-series create --data-from-stdin 2>/dev/null || true

    # Send bytes per second metric
    echo "{\"metricKind\": \"GAUGE\", \"valueType\": \"INT64\", \"timeSeries\": [{\"resource\": {\"type\": \"gce_instance\", \"labels\": {\"instance_id\": \"$INSTANCE_ID\", \"zone\": \"$ZONE\"}}, \"metric\": {\"type\": \"custom.googleapis.com/polymarket/bytes_per_sec\"}, \"points\": [{\"interval\": {\"endTime\": \"$TIMESTAMP\"}, \"value\": {\"int64Value\": \"$BYTES_SEC\"}}]}]}" | \
    gcloud monitoring time-series create --data-from-stdin 2>/dev/null || true
fi
HEALTH_EOF

chmod +x /opt/polymarket-logger/scripts/*.sh

# Create systemd service (same as AWS version)
cat > /etc/systemd/system/polymarket-logger.service << "SERVICE_EOF"
[Unit]
Description=Polymarket Data Logger
After=network.target

[Service]
Type=simple
User=polymarket
WorkingDirectory=/opt/polymarket-logger
ExecStart=/opt/polymarket-logger/feed
Restart=always
RestartSec=10
Environment=RUST_LOG=info
EnvironmentFile=/etc/environment

[Install]
WantedBy=multi-user.target
SERVICE_EOF

# Setup cron jobs
cat > /etc/cron.d/polymarket-logger << "CRON_EOF"
*/10 * * * * polymarket /opt/polymarket-logger/scripts/upload.sh
*/2 * * * * polymarket /opt/polymarket-logger/scripts/health.sh
0 6 * * * root systemctl restart polymarket-logger
CRON_EOF

systemctl daemon-reload