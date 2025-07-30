use anyhow::{Context, Result};
use std::env;
use std::io::Write;
use std::process::Command;
use std::thread;
use std::time::Duration;
use tempfile::NamedTempFile;

// Configuration constants
const ZONE: &str = "northamerica-northeast1-a";
const MACHINE_TYPE: &str = "e2-small";
const BUCKET_NAME: &str = "polymarket-data-bucket";

/// Project and zone-unique google compute engine instance name
const INSTANCE_NAME: &str = "polymarket";

/// Service name for systemd
const SERVICE_NAME: &str = "pdi";

/// Non-root user created for the service
const USER_NAME: &str = "polymarket";

/// Directory for the application binary
const APP_DIR: &str = "/opt/pdi";

/// Directory where the app puts data ready to move to the bucket
const DATA_DIR: &str = "/opt/pdi/data";

/// Name of the binary to build (match a target in Cargo.toml)
const BINARY_NAME: &str = "collector";

fn get_setup_script() -> String {
    format!(
        r#"#!/bin/bash
set -euo pipefail

# Update system
apt-get update
apt-get upgrade -y

# Install required packages
apt-get install -y build-essential pkg-config libssl-dev curl git

# Create application directory and user first
useradd -m -s /bin/bash {USER_NAME} || true

# Add sudo permissions for the polymarket user
echo "{USER_NAME} ALL=(ALL) NOPASSWD: /bin/systemctl restart {SERVICE_NAME}" > /etc/sudoers.d/{USER_NAME}
chmod 440 /etc/sudoers.d/{USER_NAME}

# Install Rust for the polymarket user
sudo -u {USER_NAME} bash -c 'curl --proto '\''=https'\'' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y'
mkdir -p {APP_DIR}
chown {USER_NAME}:{USER_NAME} {APP_DIR}

# Create data directory structure
mkdir -p {DATA_DIR}
chown -R {USER_NAME}:{USER_NAME} {DATA_DIR}

# Create systemd service
cat > /etc/systemd/system/{SERVICE_NAME}.service << EOF
[Unit]
Description=Polymarket Data Ingestor
After=network.target

[Service]
Type=simple
User={USER_NAME}
WorkingDirectory={APP_DIR}
ExecStart={APP_DIR}/{BINARY_NAME}
Restart=always
RestartSec=10
Environment=RUST_LOG=info,collector=debug,data_collector=debug
# Give the service 30 seconds to shut down gracefully
TimeoutStopSec=30

MemoryAccounting=true
MemoryMax=1024M

[Install]
WantedBy=multi-user.target
EOF

# Set up cron jobs for the polymarket user
crontab -u {USER_NAME} -l 2>/dev/null || true > /tmp/{USER_NAME}_cron
cat >> /tmp/{USER_NAME}_cron << 'CRON_EOF'
# Upload completed log files to GCS every 10 minutes
*/10 * * * * cd {APP_DIR} && gsutil -m mv {DATA_DIR}/*.jsonl.zst gs://{BUCKET_NAME}/raw/ 2>/dev/null || true

# Send metrics to Cloud Logging every 2 minutes  
*/2 * * * * journalctl -u {SERVICE_NAME} --since '2 minutes ago' | while IFS= read -r line; do gcloud logging write {SERVICE_NAME}-{BINARY_NAME} "$line" --payload-type=text; done 2>/dev/null || true

# Restart the service every 6 hours to discover new markets
0 */6 * * * sudo systemctl restart {SERVICE_NAME}
CRON_EOF

crontab -u {USER_NAME} /tmp/{USER_NAME}_cron
rm /tmp/{USER_NAME}_cron

# Enable service (will start when binary is deployed)
systemctl daemon-reload
systemctl enable {SERVICE_NAME}

echo "Setup complete!"
"#,
        USER_NAME = USER_NAME,
        APP_DIR = APP_DIR,
        DATA_DIR = DATA_DIR,
        SERVICE_NAME = SERVICE_NAME,
        BINARY_NAME = BINARY_NAME,
        BUCKET_NAME = BUCKET_NAME
    )
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <create|update|destroy|status>", args[0]);
        std::process::exit(1);
    }

    match args[1].as_str() {
        "create" => create_instance(),
        "update" => deploy_code(),
        "destroy" => destroy_instance(),
        "status" => check_status(),
        _ => {
            eprintln!("Unknown command: {}", args[1]);
            eprintln!("Available commands: create, update, destroy, status");
            std::process::exit(1);
        }
    }
}

#[rustfmt::skip]
fn create_instance() -> Result<()> {
    println!("Creating GCP instance and storage bucket...");

    // Create storage bucket
    println!("Creating storage bucket...");
    let bucket_output = Command::new("gsutil")
        .args(["mb", &format!("gs://{}", BUCKET_NAME)])
        .output()
        .context("Failed to create bucket")?;
    
    if !bucket_output.status.success() {
        let stderr = String::from_utf8_lossy(&bucket_output.stderr);
        if !stderr.contains("already exists") {
            return Err(anyhow::anyhow!("Failed to create bucket: {}", stderr));
        }
        println!("Bucket already exists, continuing...");
    }

    // Set bucket lifecycle for cost optimization
    println!("Setting bucket lifecycle rules...");
    let lifecycle_json = r#"
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
        "condition": {"age": 30}
      },
      {
        "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"}, 
        "condition": {"age": 90}
      },
      {
        "action": {"type": "SetStorageClass", "storageClass": "ARCHIVE"},
        "condition": {"age": 365}
      }
    ]
  }
}
"#;
    
    let mut temp_file = NamedTempFile::new().context("Failed to create temporary file")?;
    temp_file.write_all(lifecycle_json.as_bytes()).context("Failed to write lifecycle configuration")?;
    
    run_cmd(Command::new("gsutil")
        .args([
            "lifecycle", "set",
            temp_file.path().to_str().unwrap(),
            &format!("gs://{}", BUCKET_NAME),
        ]), "Failed to set bucket lifecycle")?;

    // Create compute instance
    println!("Creating compute instance...");
    run_cmd(Command::new("gcloud")
        .args([
            "compute", "instances", "create", INSTANCE_NAME,
            "--zone", ZONE,
            "--machine-type", MACHINE_TYPE,
            "--image-family", "ubuntu-2204-lts",
            "--image-project", "ubuntu-os-cloud",
            "--boot-disk-size", "100GB",
            "--boot-disk-type", "pd-ssd",
            "--scopes", "storage-rw,logging-write,monitoring-write",
            "--tags", INSTANCE_NAME,
            "--metadata", "enable-oslogin=true"
        ]), "Failed to create instance")?;

    // Wait for instance to be ready
    wait_for_instance_ready()?;

    // Create and copy setup script
    println!("Preparing setup script...");
    let mut temp_file = NamedTempFile::new().context("Failed to create temporary file")?;
    temp_file.write_all(get_setup_script().as_bytes()).context("Failed to write setup script")?;

    println!("Copying setup script...");
    run_cmd(Command::new("gcloud")
        .args([
            "compute", "scp",
            temp_file.path().to_str().unwrap(),
            &format!("{}:/tmp/setup.sh", INSTANCE_NAME),
            "--zone", ZONE
        ]), "Failed to copy setup script")?;

    run_cmd(&mut gcloud_ssh_cmd("sudo bash /tmp/setup.sh"), "Setup script failed")?;

    println!("✅ Instance created and configured!");
    println!("Next steps:");
    println!("  1. Run: cargo run --bin deploy -- update");
    println!("  2. Check logs and set up alerting in Cloud Console, if desired");

    Ok(())
}

#[rustfmt::skip]
fn deploy_code() -> Result<()> {
    println!("Deploying code to instance...");

    // Re-run setup script first
    println!("Re-running setup script...");
    let mut temp_file = NamedTempFile::new().context("Failed to create temporary file")?;
    temp_file.write_all(get_setup_script().as_bytes()).context("Failed to write setup script")?;

    run_cmd(Command::new("gcloud")
        .args([
            "compute", "scp",
            temp_file.path().to_str().unwrap(),
            &format!("{}:/tmp/setup.sh", INSTANCE_NAME),
            "--zone", ZONE
        ]), "Failed to copy setup script")?;

    run_cmd(&mut gcloud_ssh_cmd("sudo bash /tmp/setup.sh"), "Setup script failed")?;

    // Clean up old directory and sync source code
    println!("Cleaning up old files and syncing source code...");
    run_cmd(&mut gcloud_ssh_cmd(&format!("sudo rm -rf /tmp/{}", SERVICE_NAME)), "Failed to clean up old files")?;
    
    run_cmd(Command::new("gcloud")
        .args([
            "compute", "scp", "--recurse",
            "cli/", "collector/", "deploy/", "tests/", "Cargo.toml", "Cargo.lock",
            &format!("{}:/tmp/{}-source", INSTANCE_NAME, SERVICE_NAME),
            "--zone", ZONE
        ]), "Failed to sync source code")?;

    // Move, build and deploy
    println!("Building and deploying (first build will take up to 15 minutes on this tiny machine)...");
    let deploy_cmd = format!(
        "sudo mkdir -p /tmp/{service}-build && \
         sudo cp -r /tmp/{service}-source/* /tmp/{service}-build/ && \
         sudo chown -R {user}:{user} /tmp/{service}-build && \
         sudo -u {user} bash -c 'cd /tmp/{service}-build && source ~/.cargo/env && cargo build --release --bin {bin}' && \
         sudo systemctl stop {service} && \
         sudo cp /tmp/{service}-build/target/release/{bin} {app}/ && \
         sudo systemctl start {service}",
        user = USER_NAME, app = APP_DIR, bin = BINARY_NAME, service = SERVICE_NAME
    );
    run_cmd(&mut gcloud_ssh_cmd(&deploy_cmd), "Failed to build and deploy")?;

    // Check status
    std::thread::sleep(std::time::Duration::from_secs(3));
    println!("Checking service status...");
    run_cmd(&mut gcloud_ssh_cmd(&format!("sudo systemctl status {} --no-pager", SERVICE_NAME)), 
            "Failed to check status")?;

    println!("✅ Deployment complete!");
    println!("Monitor logs: gcloud compute ssh {} --zone {} --command 'sudo journalctl -u {} -f'", 
             INSTANCE_NAME, ZONE, SERVICE_NAME);

    Ok(())
}

#[rustfmt::skip]
fn destroy_instance() -> Result<()> {
    println!("⚠️  This will destroy the instance and all local data!");
    print!("Type 'yes' to confirm: ");
    std::io::stdout().flush()?;
    
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    
    if input.trim() != "yes" {
        println!("Cancelled.");
        return Ok(());
    }

    println!("\nDestroying instance...");
    run_cmd(Command::new("gcloud")
        .args([
            "compute", "instances", "delete", INSTANCE_NAME,
            "--zone", ZONE,
            "--quiet"
        ]), "Failed to destroy instance")?;

    println!("✅ Instance destroyed.");
    println!("Note: Bucket {} was not deleted (contains your data).", BUCKET_NAME);

    Ok(())
}

#[rustfmt::skip]
fn check_status() -> Result<()> {
    println!("Checking instance status...");
    
    println!("gcloud compute instances describe {} --zone {} --format value(status)", INSTANCE_NAME, ZONE);
    let status = Command::new("gcloud")
        .args([
            "compute", "instances", "describe", INSTANCE_NAME,
            "--zone", ZONE,
            "--format", "value(status)"
        ])
        .status();

    match status {
        Ok(s) if s.success() => {
            println!("Instance is running.");
            println!("To ssh into the instance, run: gcloud compute ssh {} --zone {}", INSTANCE_NAME, ZONE);
            
            // Check service status
            println!("Checking service status...");
            run_cmd(&mut gcloud_ssh_cmd(&format!("sudo systemctl status {} --no-pager", SERVICE_NAME)),
                    "Failed to check service status")?;
        },
        _ => {
            println!("Instance not found or not running.");
            println!("Run: cargo run --bin deploy -- create");
        }
    }

    Ok(())
}

fn wait_for_instance_ready() -> Result<()> {
    println!("Waiting for instance to be ready...");
    let interval_secs = 5;
    let max_attempts = 30;
    let mut attempts = 0;

    while attempts < max_attempts {
        let output = Command::new("gcloud")
            .args([
                "compute",
                "instances",
                "describe",
                INSTANCE_NAME,
                "--zone",
                ZONE,
                "--format",
                "value(status)",
            ])
            .output()
            .context("Failed to check instance status")?;

        let status = String::from_utf8_lossy(&output.stdout).trim().to_string();

        if status == "RUNNING" {
            // Additional check to ensure SSH is ready
            let ssh_check = Command::new("gcloud")
                .args([
                    "compute",
                    "ssh",
                    INSTANCE_NAME,
                    "--zone",
                    ZONE,
                    "--command",
                    "echo 'SSH ready'",
                ])
                .output();

            if ssh_check.is_ok() && ssh_check.unwrap().status.success() {
                println!("Instance is ready!");
                return Ok(());
            }
        }

        attempts += 1;
        if attempts < max_attempts {
            print!(".");
            std::io::stdout().flush()?;
            thread::sleep(Duration::from_secs(interval_secs));
        }
    }

    Err(anyhow::anyhow!(
        "Instance failed to become ready within timeout"
    ))
}

fn run_cmd(cmd: &mut Command, error_msg: &str) -> Result<()> {
    let status = cmd.status().with_context(|| error_msg.to_string())?;
    if !status.success() {
        return Err(anyhow::anyhow!("{}", error_msg));
    }
    Ok(())
}

fn gcloud_ssh_cmd(command: &str) -> Command {
    let mut cmd = Command::new("gcloud");
    cmd.args([
        "compute",
        "ssh",
        INSTANCE_NAME,
        "--zone",
        ZONE,
        "--command",
        command,
    ]);
    cmd
}
