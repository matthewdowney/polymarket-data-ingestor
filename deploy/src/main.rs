use anyhow::{Context, Result};
use clap::Parser;
use std::io::Write;
use std::process::Command;
use std::thread;
use std::time::Duration;
use tempfile::NamedTempFile;

#[derive(Parser, Debug)]
#[command(name = "deploy")]
struct Args {
    #[arg(value_enum)]
    venue: Venue,
    #[arg(value_enum)]
    command: DeployCommand,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum Venue {
    Polymarket,
    Kalshi,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum DeployCommand {
    Create,
    Update,
    Destroy,
    Status,
}

struct DeployConfig {
    instance_name: &'static str,
    bucket_name: &'static str,
    service_name: &'static str,
    user_name: &'static str,
    app_dir: &'static str,
    data_dir: &'static str,
    venue: &'static str,
}

impl DeployConfig {
    fn for_venue(venue: &Venue) -> Self {
        match venue {
            Venue::Polymarket => Self {
                instance_name: "polymarket",
                bucket_name: "polymarket-data-bucket",
                service_name: "pdi",
                user_name: "polymarket",
                app_dir: "/opt/pdi",
                data_dir: "/opt/pdi/data",
                venue: "polymarket",
            },
            Venue::Kalshi => Self {
                instance_name: "kalshi",
                bucket_name: "kalshi-data-bucket",
                service_name: "kdi",
                user_name: "kalshi",
                app_dir: "/opt/kdi",
                data_dir: "/opt/kdi/data",
                venue: "kalshi",
            },
        }
    }
}

// Configuration constants
const ZONE: &str = "northamerica-northeast1-a";
const MACHINE_TYPE: &str = "e2-medium";

/// Name of the binary to build (match a target in Cargo.toml)
const BINARY_NAME: &str = "collector";

fn get_setup_script(config: &DeployConfig) -> String {
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

# Add sudo permissions for the user
echo "{USER_NAME} ALL=(ALL) NOPASSWD: /bin/systemctl restart {SERVICE_NAME}" > /etc/sudoers.d/{USER_NAME}
chmod 440 /etc/sudoers.d/{USER_NAME}

# Install Rust for the user
sudo -u {USER_NAME} bash -c 'curl --proto '\''=https'\'' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y'
mkdir -p {APP_DIR}
chown {USER_NAME}:{USER_NAME} {APP_DIR}

# Create data directory structure
mkdir -p {DATA_DIR}
chown -R {USER_NAME}:{USER_NAME} {DATA_DIR}

# Raise file descriptor limits (system-wide, per-user, and for the service)
# System-wide kernel limits via sysctl.d
cat > /etc/sysctl.d/99-{SERVICE_NAME}.conf << SYSCTL_EOF
fs.file-max = 1000000
fs.nr_open = 1048576
SYSCTL_EOF
sysctl --system || true

# Ensure pam_limits is active
grep -q pam_limits.so /etc/pam.d/common-session || echo 'session required pam_limits.so' >> /etc/pam.d/common-session
grep -q pam_limits.so /etc/pam.d/common-session-noninteractive || echo 'session required pam_limits.so' >> /etc/pam.d/common-session-noninteractive

# Per-user limits for the service account
cat > /etc/security/limits.d/{USER_NAME}.conf << LIMITS_EOF
{USER_NAME} soft nofile 1048576
{USER_NAME} hard nofile 1048576
root       soft nofile 1048576
root       hard nofile 1048576
LIMITS_EOF

# Create systemd service
cat > /etc/systemd/system/{SERVICE_NAME}.service << EOF
[Unit]
Description={VENUE} Data Ingestor
After=network.target

[Service]
Type=simple
User={USER_NAME}
WorkingDirectory={APP_DIR}
ExecStart={APP_DIR}/{BINARY_NAME} {VENUE}
Restart=always
RestartSec=10
Environment=RUST_LOG=info,collector=debug,data_collector=debug
# Give the service 30 seconds to shut down gracefully
TimeoutStopSec=30

MemoryAccounting=true
MemoryMax=2048M
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
EOF

# Set up cron jobs for the user
crontab -u {USER_NAME} -l 2>/dev/null || true > /tmp/{USER_NAME}_cron
cat >> /tmp/{USER_NAME}_cron << 'CRON_EOF'
# Upload completed log files to GCS every 10 minutes
*/10 * * * * cd {APP_DIR} && gsutil -m mv {DATA_DIR}/*.jsonl.zst gs://{BUCKET_NAME}/raw/ 2>/dev/null || true

# Restart the service every 6 hours to discover new markets
25 */6 * * * sudo systemctl restart {SERVICE_NAME}
CRON_EOF

crontab -u {USER_NAME} /tmp/{USER_NAME}_cron
rm /tmp/{USER_NAME}_cron

# Enable service (will start when binary is deployed)
systemctl daemon-reload
systemctl enable {SERVICE_NAME}

echo "Setup complete!"
"#,
        USER_NAME = config.user_name,
        APP_DIR = config.app_dir,
        DATA_DIR = config.data_dir,
        SERVICE_NAME = config.service_name,
        BINARY_NAME = BINARY_NAME,
        BUCKET_NAME = config.bucket_name,
        VENUE = config.venue
    )
}

fn main() -> Result<()> {
    let args = Args::parse();
    let config = DeployConfig::for_venue(&args.venue);

    match args.command {
        DeployCommand::Create => create_instance(&config),
        DeployCommand::Update => deploy_code(&config),
        DeployCommand::Destroy => destroy_instance(&config),
        DeployCommand::Status => check_status(&config),
    }
}

#[rustfmt::skip]
fn create_instance(config: &DeployConfig) -> Result<()> {
    println!("Creating GCP instance and storage bucket...");

    // Create storage bucket
    println!("Creating storage bucket...");
    let bucket_output = Command::new("gsutil")
        .args(["mb", &format!("gs://{}", config.bucket_name)])
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
            &format!("gs://{}", config.bucket_name),
        ]), "Failed to set bucket lifecycle")?;

    // Create compute instance
    println!("Creating compute instance...");
    run_cmd(Command::new("gcloud")
        .args([
            "compute", "instances", "create", config.instance_name,
            "--zone", ZONE,
            "--machine-type", MACHINE_TYPE,
            "--image-family", "ubuntu-2204-lts",
            "--image-project", "ubuntu-os-cloud",
            "--boot-disk-size", "100GB",
            "--boot-disk-type", "pd-ssd",
            "--scopes", "storage-rw,logging-write,monitoring-write",
            "--tags", config.instance_name,
            "--metadata", "enable-oslogin=true"
        ]), "Failed to create instance")?;

    // Wait for instance to be ready
    wait_for_instance_ready(config)?;

    // Create and copy setup script
    println!("Preparing setup script...");
    let mut temp_file = NamedTempFile::new().context("Failed to create temporary file")?;
    temp_file.write_all(get_setup_script(config).as_bytes()).context("Failed to write setup script")?;

    println!("Copying setup script...");
    run_cmd(Command::new("gcloud")
        .args([
            "compute", "scp",
            temp_file.path().to_str().unwrap(),
            &format!("{}:/tmp/setup.sh", config.instance_name),
            "--zone", ZONE
        ]), "Failed to copy setup script")?;

    run_cmd(&mut gcloud_ssh_cmd(config, "sudo bash /tmp/setup.sh"), "Setup script failed")?;

    println!("✅ Instance created and configured!");
    println!("Next steps:");
    println!("  1. Run: cargo run --bin deploy -- update {}", config.venue);
    println!("  2. Check logs and set up alerting in Cloud Console, if desired");

    Ok(())
}

#[rustfmt::skip]
fn deploy_code(config: &DeployConfig) -> Result<()> {
    println!("Deploying code to instance...");

    // Re-run setup script first
    println!("Re-running setup script...");
    let mut temp_file = NamedTempFile::new().context("Failed to create temporary file")?;
    temp_file.write_all(get_setup_script(config).as_bytes()).context("Failed to write setup script")?;

    run_cmd(Command::new("gcloud")
        .args([
            "compute", "scp",
            temp_file.path().to_str().unwrap(),
            &format!("{}:/tmp/setup.sh", config.instance_name),
            "--zone", ZONE
        ]), "Failed to copy setup script")?;

    run_cmd(&mut gcloud_ssh_cmd(config, "sudo bash /tmp/setup.sh"), "Setup script failed")?;

    // Clean up old directory and sync source code
    println!("Cleaning up old files and syncing source code...");
    run_cmd(&mut gcloud_ssh_cmd(config, &format!("sudo rm -rf /tmp/{}", config.service_name)), "Failed to clean up old files")?;
    
    run_cmd(Command::new("gcloud")
        .args([
            "compute", "scp", "--recurse",
            "cli/", "collector/", "deploy/", "tests/", "Cargo.toml", "Cargo.lock",
            &format!("{}:/tmp/{}-source", config.instance_name, config.service_name),
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
        user = config.user_name, app = config.app_dir, bin = BINARY_NAME, service = config.service_name
    );
    run_cmd(&mut gcloud_ssh_cmd(config, &deploy_cmd), "Failed to build and deploy")?;

    // Check status
    std::thread::sleep(std::time::Duration::from_secs(3));
    println!("Checking service status...");
    run_cmd(&mut gcloud_ssh_cmd(config, &format!("sudo systemctl status {} --no-pager", config.service_name)), 
            "Failed to check status")?;

    println!("✅ Deployment complete!");
    println!("Monitor logs: gcloud compute ssh {} --zone {} --command 'sudo journalctl -u {} -f'", 
             config.instance_name, ZONE, config.service_name);

    Ok(())
}

#[rustfmt::skip]
fn destroy_instance(config: &DeployConfig) -> Result<()> {
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
            "compute", "instances", "delete", config.instance_name,
            "--zone", ZONE,
            "--quiet"
        ]), "Failed to destroy instance")?;

    println!("✅ Instance destroyed.");
    println!("Note: Bucket {} was not deleted (contains your data).", config.bucket_name);

    Ok(())
}

#[rustfmt::skip]
fn check_status(config: &DeployConfig) -> Result<()> {
    println!("Checking instance status...");
    
    println!("gcloud compute instances describe {} --zone {} --format value(status)", config.instance_name, ZONE);
    let status = Command::new("gcloud")
        .args([
            "compute", "instances", "describe", config.instance_name,
            "--zone", ZONE,
            "--format", "value(status)"
        ])
        .status();

    match status {
        Ok(s) if s.success() => {
            println!("Instance is running.");
            println!("To ssh into the instance, run: gcloud compute ssh {} --zone {}", config.instance_name, ZONE);
            
            // Check service status
            println!("Checking service status...");
            run_cmd(&mut gcloud_ssh_cmd(config, &format!("sudo systemctl status {} --no-pager", config.service_name)),
                    "Failed to check service status")?;
        },
        _ => {
            println!("Instance not found or not running.");
            println!("Run: cargo run --bin deploy -- create");
        }
    }

    Ok(())
}

fn wait_for_instance_ready(config: &DeployConfig) -> Result<()> {
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
                config.instance_name,
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
                    config.instance_name,
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

fn gcloud_ssh_cmd(config: &DeployConfig, command: &str) -> Command {
    let mut cmd = Command::new("gcloud");
    cmd.args([
        "compute",
        "ssh",
        config.instance_name,
        "--zone",
        ZONE,
        "--command",
        command,
    ]);
    cmd
}
