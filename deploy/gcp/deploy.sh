#!/bin/bash
set -e

# Build the binary
cd ../../
docker build -t polymarket-builder .
docker run --rm -v $(pwd):/output polymarket-builder cp /usr/src/app/target/x86_64-unknown-linux-gnu/release/feed /output/target/x86_64-unknown-linux-gnu/release/

# # Check if binary exists
# if [ ! -f "../../target/x86_64-unknown-linux-gnu/release/feed" ]; then
#     echo "❌ Build the binary first: cross build --target x86_64-unknown-linux-gnu --release"
#     exit 1
# fi
cd deploy/gcp

INSTANCE_NAME=$(terraform output -raw instance_name)
ZONE=$(terraform output -raw zone || echo "northamerica-northeast1-a")

echo "🚀 Deploying to $INSTANCE_NAME in zone $ZONE..."

# Copy binary to instance using gcloud
gcloud compute scp ../../target/x86_64-unknown-linux-gnu/release/feed $INSTANCE_NAME:/tmp/ --zone=$ZONE

# Install and start service
gcloud compute ssh $INSTANCE_NAME --zone=$ZONE --command='
# Move binary to final location
sudo mv /tmp/feed /opt/polymarket-logger/
sudo chown polymarket:polymarket /opt/polymarket-logger/feed
sudo chmod +x /opt/polymarket-logger/feed

# Start service
sudo systemctl enable polymarket-logger
sudo systemctl restart polymarket-logger

# Check status
sleep 2
sudo systemctl status polymarket-logger --no-pager
'

echo "✅ Deployment complete!"
echo "📊 Monitor: https://console.cloud.google.com/monitoring/"
echo "🔍 Check logs: gcloud compute ssh $INSTANCE_NAME --zone=$ZONE"
echo "   sudo journalctl -u polymarket-logger -f"