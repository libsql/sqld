use std::path::Path;
use std::fs::read_to_string;

use anyhow::Context;
use semver::Version as SemVer;

enum Version {
    Pre0_18,
    Named(SemVer),
}

pub fn maybe_migrate(db_path: &Path) -> anyhow::Result<()> {
    // migration is performed in steps, until the most current version is reached
    loop {
        match detect_version(db_path)? {
            Version::Pre0_18 => migrate_step_from_pre_0_18(db_path)?,
            // most recent version was reached: exit
            Version::Named(_) => return Ok(()),
        }
    }
}

fn detect_version(db_path: &Path) -> anyhow::Result<Version> {
    let version_file_path = db_path.join(".version");
    if !version_file_path.exists() {
        return Ok(Version::Pre0_18)
    }

    let version_str = read_to_string(version_file_path)?;
    let version = SemVer::parse(&version_str).context("invalid version file")?;

    Ok(Version::Named(version))
}

fn migrate_step_from_pre_0_18(db_path: &Path) -> anyhow::Result<()> {
    tracing::info!("version < 0.18.0 detected, performing migration");

    fn try_migrate(db_path: &Path) -> anyhow::Result<()> {
        std::fs::write(db_path.join(".version"), b"0.18.0")?;
        let ns_dir = db_path.join("dbs").join("default");
        std::fs::create_dir_all(&ns_dir)?;

        macro_rules! maybe_link {
            ($name:expr) => {
                if db_path.join($name).exists() {
                    std::fs::hard_link(db_path.join($name), ns_dir.join($name))?;
                }
            };
        }

        // link standalone files
        maybe_link!("data");
        maybe_link!("data-shm");
        maybe_link!("data-wal");
        maybe_link!("wallog");
        maybe_link!("client_wal_index");

        // link snapshots
        let snapshot_dir = db_path.join("snapshots");
        if snapshot_dir.exists() {
            let new_snap_dir = ns_dir.join("snapshots");
            std::fs::create_dir_all(&new_snap_dir)?;
            for entry in std::fs::read_dir(snapshot_dir)? {
                let entry = entry?;
                if let Some(name) = entry.path().file_name() {
                    std::fs::hard_link(entry.path(), new_snap_dir.join(name))?;
                }
            }
        }

        Ok(())
    }

    if let Err(e) = try_migrate(db_path) {
        let _ = std::fs::remove_dir_all(db_path.join("dbs"));
        return Err(e);
    }

    // best effort cleanup 
    let _ = std::fs::remove_file(db_path.join("data"));
    let _ = std::fs::remove_file(db_path.join("data-shm"));
    let _ = std::fs::remove_file(db_path.join("data-wal"));
    let _ = std::fs::remove_file(db_path.join("wallog"));
    let _ = std::fs::remove_file(db_path.join("client_wal_index"));

    Ok(())
}
