use aws_sdk_s3::Client;
use chrono::Utc;
use clokwerk::{AsyncScheduler, TimeUnits};
use std::env::consts::ARCH;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;
use tokio::spawn;
use tracing::{error, info, warn};
use verification::scarb_and_dojo_download_scheduler::{
    check_periodically_scarb_updates, check_periodically_sozo_updates,
};

/// The folder the bucket keys this machine's binaries under.
///
/// Note it is not `ARCH`: the bucket says "arm64" where Rust says "aarch64".
fn bucket_arch_folder() -> Result<&'static str, Box<dyn std::error::Error>> {
    match ARCH {
        "x86_64" => Ok("x86_64"),
        "aarch64" | "arm" => Ok("arm64"),
        other => Err(Box::from(format!("Unsupported architecture: {}", other))),
    }
}

/// Where a bucket object belongs on disk: `scarb/x86_64/scarb_cairo_v2.10.1`
/// becomes `<BINARIES_SAVE_DIRECTORY_PATH>/scarb/scarb_cairo_v2.10.1`. The
/// architecture segment exists only in the bucket — the verifier looks under
/// `<dir>/<tool>/<name>` (crates/verification/src/scarb.rs:162-215).
///
/// Returns `None` for any key that is not exactly `<tool>/<arch>/<name>` — which
/// also drops the empty folder markers a listing can contain — and for any key
/// belonging to a different architecture.
fn local_path_for(
    object_key: &str,
    arch_folder: &str,
    binaries_save_directory_path: &str,
) -> Option<String> {
    let mut segments = object_key.split('/');
    let tool = segments.next().filter(|s| !s.is_empty())?;
    segments.next().filter(|s| *s == arch_folder)?;
    let name = segments.next().filter(|s| !s.is_empty())?;
    if segments.next().is_some() {
        return None;
    }
    Some(format!(
        "{}/{}/{}",
        binaries_save_directory_path, tool, name
    ))
}

/// Every object under a prefix, following pagination.
///
/// Uses the V1 listing rather than `list_objects_v2` deliberately: object
/// storage here is GCS reached through its S3 interoperability API (see
/// crates/server/src/main.rs:100-109 and walnut-infra/storage.tf), and V1 with
/// a marker is the form that API is documented to support.
async fn list_bucket_objects(
    s3_client: &Client,
    prefix: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let bucket_name = std::env::var("BINARIES_S3_BUCKET_NAME").unwrap_or("./binaries".to_string());
    let mut keys: Vec<String> = Vec::new();
    let mut marker: Option<String> = None;

    loop {
        let mut request = s3_client.list_objects().bucket(&bucket_name).prefix(prefix);
        if let Some(marker) = &marker {
            request = request.marker(marker);
        }
        let response = request.send().await?;

        let page: Vec<String> = response
            .contents()
            .iter()
            .filter_map(|object| object.key().map(str::to_string))
            .collect();
        let last_key = page.last().cloned();
        keys.extend(page);

        if !response.is_truncated().unwrap_or(false) {
            break;
        }
        // `next_marker` is only returned when the request used a delimiter;
        // otherwise the caller continues from the last key it saw.
        marker = response.next_marker().map(str::to_string).or(last_key);
        if marker.is_none() {
            break;
        }
    }

    Ok(keys)
}

/// Populate the local toolchain directory from the binaries bucket.
///
/// This used to name six Scarb versions and two Sozo versions inline, on the
/// assumption that the bucket only had to cover the tail below where the GitHub
/// scheduler starts. It does not: that scheduler reads `/releases` unpaginated,
/// so it only ever sees the newest 30, and a version missing from disk that has
/// scrolled off that page is missing for good — which is how a reprovisioned
/// data disk ended up unable to verify Cairo 2.10.1 contracts. The bucket is the
/// durable copy of every toolchain the verifier can ask for, so take whatever is
/// in it and let backfill-binaries-bucket.sh decide what that is.
///
/// The whole bucket is listed rather than the `scarb/` and `sozo/` prefixes
/// specifically: naming the tools here would put the same "which versions
/// exist" decision back in the binary that the hardcoded list already got
/// wrong, just one level up. Anything keyed `<tool>/<this arch>/<name>` is
/// taken, so adding a toolchain to the bucket needs no change here.
pub async fn download_scarb_and_sozo_binaries_from_s3(
    s3_client: &Client,
) -> Result<(), Box<dyn std::error::Error>> {
    let arch_folder = bucket_arch_folder()?;
    let binaries_save_directory_path =
        std::env::var("BINARIES_SAVE_DIRECTORY_PATH").unwrap_or("".to_string());

    let keys = list_bucket_objects(s3_client, "").await?;
    info!("Bucket holds {} objects", keys.len());

    let mut downloaded = 0usize;
    let mut already_present = 0usize;
    let mut other_architecture = 0usize;

    for key in keys {
        if local_path_for(&key, arch_folder, &binaries_save_directory_path).is_none() {
            other_architecture += 1;
            continue;
        }
        if download_binary(s3_client, &key).await? {
            downloaded += 1;
        } else {
            already_present += 1;
        }
    }

    if downloaded == 0 && already_present == 0 {
        warn!(
            "The binaries bucket holds nothing for {} — verification builds will fail until it is backfilled",
            arch_folder
        );
    } else {
        info!(
            "Toolchains from the bucket for {}: {} downloaded, {} already on disk, {} for other architectures",
            arch_folder, downloaded, already_present, other_architecture
        );
    }

    Ok(())
}

// Downloads the binary from the bucket, saves it to the local directory and
// gives it executable permissions. Returns whether anything was downloaded.
async fn download_binary(
    s3_client: &Client,
    object_key: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let bucket_name = std::env::var("BINARIES_S3_BUCKET_NAME").unwrap_or("./binaries".to_string());
    let binaries_save_directory_path =
        std::env::var("BINARIES_SAVE_DIRECTORY_PATH").unwrap_or("".to_string());

    let local_file_path = match local_path_for(
        object_key,
        bucket_arch_folder()?,
        &binaries_save_directory_path,
    ) {
        Some(path) => path,
        None => {
            warn!(
                "Ignoring unexpected object key in the bucket: {}",
                object_key
            );
            return Ok(false);
        }
    };

    // Check if the file already exists
    if Path::new(&local_file_path).exists() {
        info!(
            "File already exists (skipping download): {}",
            local_file_path
        );
        return Ok(false); // Exit early if the file exists
    }
    info!("Downloading object: {}/{}", bucket_name, object_key);

    // Fetch the object from the S3 bucket
    let resp = match s3_client
        .get_object()
        .bucket(bucket_name)
        .key(object_key)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            // If the object simply isn't in the bucket (404 NoSuchKey), don't crash the
            // server — just log it and skip this binary. Other errors (auth, network,
            // wrong region/endpoint) still bubble up, since those mean S3 is misconfigured.
            if err
                .as_service_error()
                .map(|e| e.is_no_such_key())
                .unwrap_or(false)
            {
                warn!("Binary not found in S3 (skipping download): {}", object_key);
                return Ok(false);
            }
            return Err(err.into());
        }
    };
    // Ensure the directory exists
    if let Some(parent_dir) = Path::new(&local_file_path).parent() {
        fs::create_dir_all(parent_dir)?;
    }

    // Write beside the destination and rename into place, so an interrupted
    // startup cannot leave a half-written file that later runs — which only
    // check for existence — mistake for an installed toolchain.
    let partial_file_path = format!("{}.partial", local_file_path);
    let mut file = File::create(&partial_file_path)?;

    // Stream the object content to the file
    let data = resp.body.collect().await?;
    file.write_all(&data.into_bytes())?;

    let mut permissions = fs::metadata(&partial_file_path)?.permissions();
    permissions.set_mode(0o755); // rwxr-xr-x
    fs::set_permissions(&partial_file_path, permissions)?;
    fs::rename(&partial_file_path, &local_file_path)?;

    info!("Object downloaded successfully to: {}", local_file_path);

    Ok(true)
}

pub async fn start_github_scarb_binaries_downloader_scheduler() {
    start_downloader_scheduler(
        "scarb".to_string(),
        "SCARB_GITHUB_REPO_NAME".to_string(),
        "SCARB_LATEST_VERSION_FILE_NAME".to_string(),
        "SCARB_RUN_SCHEDULER_INTERVAL_MINUTES".to_string(),
    )
    .await;
}

pub async fn start_github_dojo_binaries_downloader_scheduler() {
    start_downloader_scheduler(
        "sozo".to_string(),
        "DOJO_GITHUB_REPO_NAME".to_string(),
        "DOJO_LATEST_VERSION_FILE_NAME".to_string(),
        "DOJO_RUN_SCHEDULER_INTERVAL_MINUTES".to_string(),
    )
    .await;
}

// 1. Runs immidiately after app startup
// 2. Then runs every X minutes (60 by default)
pub async fn start_downloader_scheduler(
    tool_name: String,
    repo_env_var: String,
    versioning_file_name_env_var: String,
    interval_env_var: String,
) {
    let interval: u32 = std::env::var(&interval_env_var)
        .unwrap_or_else(|_| "60".to_string())
        .parse::<u32>()
        .unwrap();

    let mut scheduler = AsyncScheduler::with_tz(Utc);
    info!(
        "Starting {} binaries downloader scheduler. Checking every: {} minutes",
        &tool_name, &interval
    );

    run_task(
        tool_name.as_ref(),
        repo_env_var.as_ref(),
        versioning_file_name_env_var.as_ref(),
    )
    .await;

    scheduler.every(interval.minutes()).run(move || {
        let name = tool_name.clone();
        let repo_env_var = repo_env_var.clone();
        let versioning_file_name_env_var = versioning_file_name_env_var.clone();
        async move {
            run_task(
                name.as_ref(),
                repo_env_var.as_ref(),
                versioning_file_name_env_var.as_ref(),
            )
            .await;
        }
    });

    spawn(async move {
        loop {
            scheduler.run_pending().await;
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });
}

async fn run_task(tool_name: &str, repo_env_var: &str, versioning_file_name_env_var: &str) {
    info!("Starting {} update check", tool_name);

    let repo = match std::env::var(repo_env_var) {
        Ok(value) => value,
        Err(_) => {
            error!("Environment variable {} is not set", repo_env_var);
            return;
        }
    };

    let versioning_file_name = match std::env::var(versioning_file_name_env_var) {
        Ok(value) => value,
        Err(_) => {
            error!(
                "Environment variable {} is not set",
                versioning_file_name_env_var
            );
            return;
        }
    };

    let res = match tool_name {
        "scarb" => {
            check_periodically_scarb_updates(repo.as_ref(), versioning_file_name.as_ref()).await
        }
        "sozo" => {
            check_periodically_sozo_updates(repo.as_ref(), versioning_file_name.as_ref()).await
        }
        _ => {
            error!("Unknown tool name: {}", &tool_name);
            return;
        }
    };

    match res {
        Ok(_) => info!("Finished {} update check", tool_name),
        Err(err) => error!("Error in {} update check: {:?}", tool_name, err),
    }
}

#[cfg(test)]
mod tests {
    use super::local_path_for;

    #[test]
    fn maps_a_bucket_key_to_the_path_the_verifier_reads() {
        assert_eq!(
            local_path_for(
                "scarb/x86_64/scarb_cairo_v2.10.1",
                "x86_64",
                "/opt/app/binaries"
            ),
            Some("/opt/app/binaries/scarb/scarb_cairo_v2.10.1".to_string())
        );
    }

    #[test]
    fn strips_the_arch_segment_whatever_it_is_called() {
        // The bucket says "arm64" where Rust says "aarch64". The segment is
        // dropped by position rather than by name so the two cannot drift —
        // matching on ARCH used to leave arm binaries in a directory the
        // verifier never looks in.
        assert_eq!(
            local_path_for("sozo/arm64/sozo_v1.0.1", "arm64", "/opt/app/binaries"),
            Some("/opt/app/binaries/sozo/sozo_v1.0.1".to_string())
        );
    }

    #[test]
    fn rejects_anything_that_is_not_tool_arch_name() {
        assert_eq!(local_path_for("scarb/x86_64/", "x86_64", "/binaries"), None);
        assert_eq!(local_path_for("scarb/x86_64", "x86_64", "/binaries"), None);
        assert_eq!(
            local_path_for("scarb/x86_64/nested/scarb", "x86_64", "/binaries"),
            None
        );
        assert_eq!(local_path_for("", "x86_64", "/binaries"), None);
    }

    #[test]
    fn ignores_objects_belonging_to_another_architecture() {
        // The whole bucket is listed now, so this filter is the only thing
        // keeping an arm build off an x86 machine.
        assert_eq!(
            local_path_for("scarb/arm64/scarb_cairo_v2.10.1", "x86_64", "/binaries"),
            None
        );
    }
}
