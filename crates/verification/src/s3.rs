use crate::db::{fetch_verified_class, fetch_verified_class_with_inlining_class};
use crate::SierraToCairoDebugInfo;
use crate::{VerifiedClassData, VerifiedClassRow};
use anyhow::Result;
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_types::body::SdkBody;
use cairo_lang_starknet_classes::contract_class::ContractClass;
use sqlx::{Pool, Postgres};
use std::collections::{HashMap, HashSet};
use tracing::error;

/// Bucket holding the verified class payloads (`class-<class_hash>.json`), as a
/// `&str` for the S3 builder calls.
///
/// `CLASSES_S3_BUCKET_NAME` is required; `check_required_env` forces it at
/// startup so a missing value stops the process rather than panicking on the
/// first class fetch.
pub fn classes_bucket_name() -> &'static str {
    walnut_shared::CLASSES_S3_BUCKET_NAME.as_str()
}

pub fn key_for_class_hash(class_hash: &str) -> String {
    format!("class-{}.json", class_hash)
}

pub async fn fetch_verified_class_hash_with_contract_class_data(
    db_pool: &Pool<Postgres>,
    s3_client: &aws_sdk_s3::Client,
    class_hash: &str,
) -> Result<(String, Option<ContractClass>)> {
    let verified_class = fetch_verified_class_with_inlining_class(db_pool, class_hash).await?;

    let primary_class_hash = &verified_class.0;
    let bucket_name = classes_bucket_name();

    // First try to fetch class data with inline_class_hash, if we have it
    if let Some(inline_class_hash) = &verified_class.1 {
        let key = key_for_class_hash(inline_class_hash);
        if let Ok(verified_class_data) = fetch_and_parse_file(s3_client, bucket_name, key).await {
            return Ok((
                primary_class_hash.clone(),
                Some(verified_class_data.contract_class),
            ));
        }
    }

    // If inline_class_hash does not exist, or there is no class data on s3, try with primary_class_hash
    let key = key_for_class_hash(primary_class_hash);
    let contract_class_data = fetch_and_parse_file(s3_client, bucket_name, key)
        .await
        .ok()
        .map(|d| d.contract_class);

    Ok((primary_class_hash.clone(), contract_class_data))
}

pub async fn fetch_verified_class_hash_with_source_code_data(
    db_pool: &Pool<Postgres>,
    s3_client: &aws_sdk_s3::Client,
    class_hash: &str,
) -> Result<Option<HashMap<String, String>>> {
    let verified_class = fetch_verified_class_with_inlining_class(db_pool, class_hash).await?;

    let primary_class_hash = &verified_class.0;
    let bucket_name = classes_bucket_name();

    // First try to fetch class data with inline_class_hash, if we have it
    if let Some(inline_class_hash) = &verified_class.1 {
        let key = key_for_class_hash(inline_class_hash);
        if let Ok(verified_class_data) = fetch_and_parse_file(s3_client, bucket_name, key).await {
            return Ok(Some(verified_class_data.source_code));
        }
    }

    // If inline_class_hash does not exist, or there is no class data on s3, try with primary_class_hash
    let key = key_for_class_hash(primary_class_hash);
    let source_code_data = fetch_and_parse_file(s3_client, bucket_name, key)
        .await
        .ok()
        .map(|d| d.source_code);

    Ok(source_code_data)
}

pub async fn fetch_verified_class_with_data(
    db_pool: &Pool<Postgres>,
    s3_client: &aws_sdk_s3::Client,
    class_hash: &String,
) -> Result<(VerifiedClassRow, VerifiedClassData)> {
    let verified_class = fetch_verified_class(db_pool, class_hash).await?;

    let resp = s3_client
        .get_object()
        .bucket(classes_bucket_name())
        .key(key_for_class_hash(class_hash))
        .send()
        .await?;

    let body = resp.body.collect().await?;
    let mut parsed: VerifiedClassData = serde_json::from_slice(&body.into_bytes())?;

    // Filter out unused files
    if let Some(cairo_debug_info) = &parsed.cairo_debug_info {
        let mut used_files: HashSet<String> = cairo_debug_info
            .sierra_statements_to_cairo_info
            .values()
            .flat_map(|info| info.cairo_locations.iter().map(|loc| loc.file_path.clone()))
            .collect();

        used_files.insert("Scarb.toml".to_string());

        parsed
            .source_code
            .retain(|file_path, _| used_files.contains(file_path));
    }

    Ok((verified_class, parsed))
}

async fn fetch_and_parse_file(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: String,
) -> Result<VerifiedClassData> {
    let resp = client
        .get_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await?;

    let body = resp.body.collect().await?;
    let parsed: VerifiedClassData = serde_json::from_slice(&body.into_bytes())?;
    Ok(parsed)
}

/// Fetches the source code for a single class.
pub async fn fetch_class_source_code(
    s3_client: &aws_sdk_s3::Client,
    class_hash: &String,
) -> Result<HashMap<String, String>> {
    let verified_class_data = match fetch_and_parse_file(
        s3_client,
        classes_bucket_name(),
        key_for_class_hash(class_hash),
    )
    .await
    {
        Ok(data) => data,
        Err(e) => {
            let error_message = format!(
                "Failed to fetch and parse file for class {}: {:?}",
                class_hash, e
            );
            error!(error_message);
            return Err(anyhow::anyhow!(error_message));
        }
    };

    Ok(verified_class_data.source_code)
}

pub async fn upload_class_to_s3(
    s3_client: &aws_sdk_s3::Client,
    class_hash: &str,
    contract_class: &ContractClass,
    cairo_debug_info: &Option<SierraToCairoDebugInfo>,
    source_code: &HashMap<String, String>,
) -> Result<()> {
    let verified_class_data = VerifiedClassData {
        contract_class: contract_class.clone(),
        cairo_debug_info: cairo_debug_info.clone(),
        source_code: source_code.clone(),
    };

    let json_data = serde_json::to_string(&verified_class_data)?;
    s3_client
        .put_object()
        .bucket(classes_bucket_name())
        .key(format!("class-{}.json", class_hash))
        .body(ByteStream::new(SdkBody::from(json_data)))
        .send()
        .await?;

    Ok(())
}
