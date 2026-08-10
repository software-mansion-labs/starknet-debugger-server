pub mod abi;
pub mod abi_processor;
pub mod felt252_serde;
pub mod felt252_vec_compression;
pub mod utils;
use anyhow::{anyhow, Context, Result};
use blockifier::execution::syscalls::hint_processor::ENTRYPOINT_FAILED_ERROR;
use ethers::providers::{Http, Provider as EthProvider};
use ethers::types::H256;
use num_bigint::{BigInt, BigUint};
use num_integer::Integer;
use num_traits::Num;
use serde::Serialize;
use serde_json::Value;
use starknet_api::block::GasPrice;
use starknet_api::core::ChainId;
use starknet_api::execution_resources::GasAmount;
use starknet_api::transaction::fields::{AllResourceBounds, ResourceBounds, ValidResourceBounds};
use starknet_rust::core::types::{
    BlockId, FeePayment, Felt, MaybePreConfirmedBlockWithTxHashes, PriceUnit, ResourceBoundsMapping,
};
use starknet_rust::providers::{
    jsonrpc::{HttpTransport, JsonRpcClient},
    Provider, Url,
};
use starknet_selector_decoder::get_selector;
use starknet_types_core::felt::CAIRO_PRIME_BIGINT;
use std::fmt;
use std::str::FromStr;
use std::sync::LazyLock;

#[derive(Serialize, Debug, Clone)]
pub struct Parameter {
    pub name: String,
    pub type_name: String,
}

pub const MAINNET_CHAIN_ID: &str = "0x534e5f4d41494e";
pub const SEPOLIA_CHAIN_ID: &str = "0x534e5f5345504f4c4941";

pub const STRK_FEE_TOKEN_ADDRESS: &str =
    "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d";
pub const ETH_FEE_TOKEN_ADDRESS: &str =
    "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7";
// RPC URLs come from the environment. They carry a paid provider API key, so
// they must never be written into the source. `check_required_env` at startup
// makes a missing one fail right away instead of on the first request.
pub static STARKNET_MAINNET_RPC_URL: LazyLock<String> =
    LazyLock::new(|| required_env("STARKNET_MAINNET_RPC_URL"));
pub static STARKNET_SEPOLIA_RPC_URL: LazyLock<String> =
    LazyLock::new(|| required_env("STARKNET_SEPOLIA_RPC_URL"));
pub static ETHEREUM_MAINNET_RPC_URL: LazyLock<String> =
    LazyLock::new(|| required_env("ETHEREUM_MAINNET_RPC_URL"));
pub static ETHEREUM_SEPOLIA_RPC_URL: LazyLock<String> =
    LazyLock::new(|| required_env("ETHEREUM_SEPOLIA_RPC_URL"));
// Bucket holding the verified class payloads (`class-<class_hash>.json`).
// Required rather than defaulted on purpose: pointing at the wrong bucket does
// not fail, it silently degrades every class to unverified, which costs far
// more to diagnose than a missing variable at startup.
pub static CLASSES_S3_BUCKET_NAME: LazyLock<String> =
    LazyLock::new(|| required_env("CLASSES_S3_BUCKET_NAME"));

fn required_env(name: &str) -> String {
    let value = std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} env var must be set (see .env.example)"));
    if value.is_empty() {
        panic!("{name} env var must not be empty (see .env.example)");
    }
    value
}

/// Reads every required env var so the process stops at startup if one is
/// missing. Call this from `main` before serving requests.
pub fn check_required_env() {
    LazyLock::force(&STARKNET_MAINNET_RPC_URL);
    LazyLock::force(&STARKNET_SEPOLIA_RPC_URL);
    LazyLock::force(&ETHEREUM_MAINNET_RPC_URL);
    LazyLock::force(&ETHEREUM_SEPOLIA_RPC_URL);
    LazyLock::force(&CLASSES_S3_BUCKET_NAME);
}

#[derive(Debug, Clone, PartialEq)]
pub enum ENetwork {
    Starknet,
    Ethereum,
}

impl fmt::Display for ENetwork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let network_str = match self {
            ENetwork::Starknet => "starknet",
            ENetwork::Ethereum => "ethereum",
        };
        write!(f, "{}", network_str)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum EChainId {
    StarknetMainnet,
    StarknetSepolia,
    EthereumMainnet,
    EthereumSepolia,
}

impl From<EChainId> for ChainId {
    fn from(e_chain: EChainId) -> Self {
        match e_chain {
            EChainId::StarknetMainnet => ChainId::Mainnet,
            EChainId::StarknetSepolia => ChainId::Sepolia,
            EChainId::EthereumMainnet => ChainId::Other("eth_mainnet".into()),
            EChainId::EthereumSepolia => ChainId::Other("eth_sepolia".into()),
        }
    }
}

impl TryFrom<ChainId> for EChainId {
    type Error = ();

    fn try_from(chain: ChainId) -> Result<Self, Self::Error> {
        match chain {
            ChainId::Mainnet => Ok(EChainId::StarknetMainnet),
            ChainId::Sepolia => Ok(EChainId::StarknetSepolia),
            ChainId::Other(ref s) if s == "eth_mainnet" => Ok(EChainId::EthereumMainnet),
            ChainId::Other(ref s) if s == "eth_sepolia" => Ok(EChainId::EthereumSepolia),
            _ => Err(()),
        }
    }
}

pub fn create_rpc_client(chain_id: &ChainId) -> JsonRpcClient<HttpTransport> {
    JsonRpcClient::new(HttpTransport::new(rpc_url(chain_id)))
}

pub fn create_rpc_client_from_url(rpc_url: Url) -> JsonRpcClient<HttpTransport> {
    JsonRpcClient::new(HttpTransport::new(rpc_url))
}

pub fn rpc_url(chain_id: &ChainId) -> Url {
    match chain_id {
        ChainId::Mainnet => Url::parse(&STARKNET_MAINNET_RPC_URL).unwrap(),
        ChainId::Sepolia => Url::parse(&STARKNET_SEPOLIA_RPC_URL).unwrap(),
        _ => panic!("Invalid chain id"),
    }
}

pub fn create_eth_provider_from_url(rpc_url: String) -> EthProvider<Http> {
    EthProvider::<Http>::try_from(rpc_url).expect("Not valid Ethereum RPC URL")
}

pub fn eth_rpc_url(chain_id: &ChainId) -> String {
    match chain_id {
        ChainId::Mainnet => ETHEREUM_MAINNET_RPC_URL.to_string(),
        ChainId::Sepolia => ETHEREUM_SEPOLIA_RPC_URL.to_string(),
        _ => panic!("Invalid chain id"),
    }
}

pub fn get_rpc_urls(chain_id: &EChainId) -> (Option<Url>, Option<String>) {
    match chain_id {
        EChainId::StarknetMainnet => (
            Url::parse(&STARKNET_MAINNET_RPC_URL).ok(),
            Some(ETHEREUM_MAINNET_RPC_URL.to_string()),
        ),
        EChainId::StarknetSepolia => (
            Url::parse(&STARKNET_SEPOLIA_RPC_URL).ok(),
            Some(ETHEREUM_SEPOLIA_RPC_URL.to_string()),
        ),
        EChainId::EthereumMainnet => (
            Url::parse(&STARKNET_MAINNET_RPC_URL).ok(),
            Some(ETHEREUM_MAINNET_RPC_URL.to_string()),
        ),
        EChainId::EthereumSepolia => (
            Url::parse(&STARKNET_SEPOLIA_RPC_URL).ok(),
            Some(ETHEREUM_SEPOLIA_RPC_URL.to_string()),
        ),
    }
}

pub fn mask_private_token_in_url<T: AsRef<str>>(raw_url: T) -> String {
    let raw_url_str = raw_url.as_ref();

    // If url contains "public" no changes
    if raw_url_str.contains("public") {
        return raw_url_str.to_string();
    }

    let mut url = match Url::parse(raw_url_str) {
        Ok(u) => u,
        Err(_) => return raw_url_str.to_string(),
    };

    if let Some(host) = url.host_str() {
        if host.contains("alchemy") || host.contains("infura") || host.contains("blast") {
            let parts: Vec<_> = url
                .path_segments()
                .map(|segments| segments.collect::<Vec<_>>())
                .unwrap_or_default();

            if !parts.is_empty() {
                let mut new_parts = parts[..parts.len() - 1].to_vec();
                new_parts.push("PRIVATE_TOKEN");
                url.set_path(&new_parts.join("/"));
            }

            url.set_query(None);
        }
    }

    url.to_string()
}

#[derive(Debug, Clone, Copy)]
pub enum ETransactionHashType {
    Starknet(Felt),
    Ethereum(H256),
}

pub fn parse_transaction_hash_per_network(
    tx_hash: &str,
    network: &ENetwork,
) -> Option<ETransactionHashType> {
    if !is_valid_transaction_hash_for_network(tx_hash, network) {
        return None;
    }

    let trimmed = tx_hash.strip_prefix("0x")?;

    match network {
        ENetwork::Starknet => {
            let bigint_value = BigInt::from_str_radix(trimmed, 16).ok()?;
            Some(ETransactionHashType::Starknet(Felt::from(bigint_value)))
        }
        ENetwork::Ethereum => {
            let eth_hash = H256::from_str(&format!("0x{}", trimmed)).ok()?;
            Some(ETransactionHashType::Ethereum(eth_hash))
        }
    }
}

pub fn is_valid_starknet_transaction_hash(tx_hash: &str) -> bool {
    tx_hash
        .strip_prefix("0x")
        .and_then(|trimmed| BigInt::from_str_radix(trimmed, 16).ok())
        .is_some_and(|value| value < *CAIRO_PRIME_BIGINT)
}

pub fn is_valid_transaction_hash_for_network(tx_hash: &str, network: &ENetwork) -> bool {
    match network {
        ENetwork::Starknet => is_valid_starknet_transaction_hash(tx_hash),
        ENetwork::Ethereum => tx_hash.starts_with("0x") && tx_hash.len() <= 66,
    }
}

pub fn get_voyager_api_url(chain_id: &ChainId) -> Option<&str> {
    match chain_id {
        ChainId::Mainnet => Some("https://api.voyager.online/beta/"),
        ChainId::Sepolia => Some("https://sepolia-api.voyager.online/beta/"),
        _ => None,
    }
}

/// The Voyager API key, if one is configured. Voyager lookups are optional —
/// without a key the server just skips them instead of failing to start.
pub fn voyager_api_key() -> Option<String> {
    std::env::var("VOYAGER_API_KEY")
        .ok()
        .filter(|key| !key.is_empty())
}

/// Fetches the human-readable name (alias) Voyager has registered for a
/// deployed contract. Tries `contractAlias` first, falls back to `classAlias`.
/// Returns `None` if Voyager doesn't know the contract or the call fails.
pub async fn fetch_voyager_contract_alias(
    voyager_api_url: &str,
    contract_address: &str,
) -> Option<String> {
    let api_key = voyager_api_key()?;
    let client = reqwest::Client::new();
    let url = format!("{}contracts/{}", voyager_api_url, contract_address);
    let call_result = client.get(&url).header("x-api-key", api_key).send().await;
    match call_result {
        Ok(response) => {
            if response.status().is_success() {
                let contract_details: Value = response.json().await.ok()?;
                match contract_details["contractAlias"].as_str() {
                    Some(alias) => Some(alias.to_string()),
                    None => contract_details["classAlias"]
                        .as_str()
                        .map(|inner_alias| inner_alias.to_string()),
                }
            } else {
                None
            }
        }
        Err(_) => None,
    }
}

pub fn extract_chain_id(chain_id: &str) -> anyhow::Result<(EChainId, ENetwork)> {
    match chain_id.to_lowercase().as_str() {
        // Starknet
        MAINNET_CHAIN_ID | "sn_mainnet" | "sn_main" | "SN_MAINNET" => {
            Ok((EChainId::StarknetMainnet, ENetwork::Starknet))
        }
        SEPOLIA_CHAIN_ID | "sn_sepolia" | "SN_SEPOLIA" => {
            Ok((EChainId::StarknetSepolia, ENetwork::Starknet))
        }

        // Ethereum
        "eth_mainnet" | "ETH_MAINNET" => Ok((EChainId::EthereumMainnet, ENetwork::Ethereum)),
        "eth_sepolia" | "ETH_SEPOLIA" => Ok((EChainId::EthereumSepolia, ENetwork::Ethereum)),

        _ => Err(anyhow!("Invalid chain id")),
    }
}

pub fn chain_id_to_readable_string(chain_id: &ChainId) -> String {
    match chain_id {
        ChainId::Mainnet => String::from("sn_mainnet"),
        ChainId::Sepolia => String::from("sn_sepolia"),
        ChainId::Other(chain_id) => chain_id.clone(),
        _ => panic!("Invalid chain id"),
    }
}

/// Converts ChainId to URL-friendly format used in frontend URLs
pub fn chain_id_to_url_format(chain_id: &ChainId) -> String {
    match chain_id {
        ChainId::Mainnet => String::from("SN_MAINNET"),
        ChainId::Sepolia => String::from("SN_SEPOLIA"),
        ChainId::Other(chain_id) => chain_id.clone(),
        _ => panic!("Invalid chain id"),
    }
}

/// Base URL of the web app the deep links point to. Override it with
/// `WALNUT_APP_BASE_URL` when you run your own frontend; the default is the
/// hosted Walnut app. A trailing slash is dropped so the links don't end up
/// with a double one.
static WALNUT_APP_URL: LazyLock<String> = LazyLock::new(|| {
    let url = std::env::var("WALNUT_APP_BASE_URL")
        .ok()
        .filter(|url| !url.is_empty())
        .unwrap_or_else(|| DEFAULT_WALNUT_APP_URL.to_string());
    url.trim_end_matches('/').to_string()
});

const DEFAULT_WALNUT_APP_URL: &str = "https://app.walnut.dev";

/// Base URL of the web app, without a trailing slash.
pub fn walnut_app_url() -> &'static str {
    &WALNUT_APP_URL
}

pub fn simulation_deep_link(
    sender_address: &str,
    calldata: &[String],
    transaction_version: usize,
    chain_id: &ChainId,
    block_number: Option<u64>,
    rpc_url: &str,
) -> String {
    let calldata_csv = calldata.join(",");
    let calldata_joined = urlencoding::encode(&calldata_csv);
    let mut params = vec![
        format!("senderAddress={sender_address}"),
        format!("calldata={calldata_joined}"),
        format!("transactionVersion={transaction_version}"),
        format!("chainId={}", chain_id_to_url_format(chain_id)),
    ];
    if let Some(bn) = block_number {
        params.push(format!("blockNumber={bn}"));
    }
    if !matches!(chain_id, ChainId::Mainnet | ChainId::Sepolia) {
        params.push(format!("rpcUrl={}", urlencoding::encode(rpc_url)));
    }
    format!("{}/simulations?{}", walnut_app_url(), params.join("&"))
}

pub fn transaction_deep_link(
    chain_id_url: Option<&str>,
    rpc_url: Option<&str>,
    tx_hash: &str,
) -> String {
    match chain_id_url {
        Some(chain) => format!(
            "{}/transactions?chainId={chain}&txHash={tx_hash}",
            walnut_app_url()
        ),
        None => {
            let rpc = urlencoding::encode(rpc_url.unwrap_or(""));
            format!(
                "{}/transactions?rpcUrl={rpc}&txHash={tx_hash}",
                walnut_app_url()
            )
        }
    }
}

pub fn to_chain_id(chain_id: &EChainId) -> EChainId {
    match chain_id {
        EChainId::EthereumMainnet => EChainId::StarknetMainnet,
        EChainId::EthereumSepolia => EChainId::StarknetSepolia,
        EChainId::StarknetMainnet => EChainId::EthereumMainnet,
        EChainId::StarknetSepolia => EChainId::EthereumSepolia,
    }
}

pub fn bytes_to_text(bytes: [u8; 32]) -> Result<String, std::str::Utf8Error> {
    let mut text = std::str::from_utf8(&bytes)?.to_string();
    text.retain(|c| c != '\0');
    Ok(text)
}

pub fn felt_vec_to_hex_vec(felt_array: Vec<Felt>) -> Vec<String> {
    let hex_representation = felt_array
        .iter()
        .map(|felt| felt.to_fixed_hex_string())
        .collect::<Vec<String>>();
    hex_representation
}

pub fn felts_to_string(felts: &[Felt]) -> String {
    //NOTE: The foundry push this string in case of execution revert, so we want to clean it from
    //response error messagr
    let entrypoint_failed = match Felt::from_hex(ENTRYPOINT_FAILED_ERROR) {
        Ok(val) => val,
        Err(_) => return String::from("ENTRYPOINT_FAILED"),
    };
    if felts.len() == 1 && felts[0] == entrypoint_failed {
        return "Entrypoint failed".to_string();
    }

    if felts.contains(&entrypoint_failed) {
        let filtered_felts: Vec<_> = felts
            .iter()
            .cloned()
            .filter(|f| f != &entrypoint_failed)
            .collect();
        return felts_to_string(&filtered_felts);
    }

    let mut result = String::new();
    let mut pending_text = String::new();

    for felt in felts {
        let hex_str = format!("{:x}", felt);

        if let Ok(bytes) = hex::decode(&hex_str) {
            if bytes.iter().all(|&b| b.is_ascii() && !b.is_ascii_control()) {
                let text = String::from_utf8_lossy(&bytes).into_owned();

                if !pending_text.is_empty() {
                    pending_text.push_str(&text);
                    continue;
                }
                pending_text = text;
                continue;
            }
        }

        if !pending_text.is_empty() {
            if !result.is_empty() {
                result.push(' ');
            }
            result.push_str(&pending_text);
            pending_text.clear();
        }

        if !result.is_empty() {
            result.push(' ');
        }
    }

    if !pending_text.is_empty() {
        if !result.is_empty() {
            result.push(' ');
        }
        result.push_str(&pending_text);
    }

    result
}

pub fn get_contract_call_id(
    parent_contract_call_id: Option<&str>,
    contract_call_index: usize,
) -> String {
    match parent_contract_call_id {
        Some(parent_contract_call_id) => {
            format!("{}-{}", parent_contract_call_id, contract_call_index)
        }
        None => contract_call_index.to_string(),
    }
}

pub fn get_internal_function_call_id(contract_call_id: &str, fp: usize) -> String {
    format!("{}-fp-{}", contract_call_id, fp)
}

pub fn resource_bounds_mapping_to_valid_resource_bounds(
    resource_bounds_mapping: &ResourceBoundsMapping,
) -> ValidResourceBounds {
    let l1_gas = ResourceBounds {
        max_amount: GasAmount(resource_bounds_mapping.l1_gas.max_amount),
        max_price_per_unit: GasPrice(resource_bounds_mapping.l1_gas.max_price_per_unit),
    };

    let l1_data_gas = ResourceBounds {
        max_amount: GasAmount(resource_bounds_mapping.l1_data_gas.max_amount),
        max_price_per_unit: GasPrice(resource_bounds_mapping.l1_data_gas.max_price_per_unit),
    };

    let l2_gas = ResourceBounds {
        max_amount: GasAmount(resource_bounds_mapping.l2_gas.max_amount),
        max_price_per_unit: GasPrice(resource_bounds_mapping.l2_gas.max_price_per_unit),
    };

    if l2_gas.is_zero() {
        ValidResourceBounds::L1Gas(l1_gas)
    } else {
        ValidResourceBounds::AllResources(AllResourceBounds {
            l1_gas,
            l2_gas,
            l1_data_gas,
        })
    }
}

pub fn resource_bounds_mapping_to_default_valid_resource_bounds() -> ValidResourceBounds {
    const MAX_GAS: u64 = u64::MAX;

    let max_bounds = ResourceBounds {
        max_amount: GasAmount(MAX_GAS),
        max_price_per_unit: GasPrice(MAX_GAS.into()),
    };
    ValidResourceBounds::AllResources(AllResourceBounds {
        l1_gas: max_bounds,
        l2_gas: max_bounds,
        l1_data_gas: max_bounds,
    })
}

pub fn get_name_of_entry_point_selector(entry_point_selector: &Felt) -> Option<String> {
    let entry_point_selector_str = entry_point_selector.to_fixed_hex_string();
    get_selector(&entry_point_selector_str).map(|name| name.to_string())
}

pub fn parse_version_string_to_tuple(version: &str) -> Result<(u32, u32, u32)> {
    // Remove the leading 'v' if present
    let version_str = version.trim_start_matches('v');

    // Split the version string by '.'
    let parts: Vec<&str> = version_str.split('.').collect();

    // Ensure there are exactly three parts
    if parts.len() != 3 {
        return Err(anyhow::anyhow!("Invalid version string format"));
    }

    // Parse each part as a u32
    let major = parts[0]
        .parse::<u32>()
        .map_err(|_| anyhow::anyhow!("Invalid major version"))?;
    let minor = parts[1]
        .parse::<u32>()
        .map_err(|_| anyhow::anyhow!("Invalid minor version"))?;
    let patch = parts[2]
        .parse::<u32>()
        .map_err(|_| anyhow::anyhow!("Invalid patch version"))?;

    Ok((major, minor, patch))
}

pub fn tuple_to_version_string(version_tuple: (u32, u32, u32)) -> String {
    format!(
        "{}.{}.{}",
        version_tuple.0, version_tuple.1, version_tuple.2
    )
}

pub fn extract_cairo_version_from_program(program: &[Felt]) -> Result<(u32, u32, u32)> {
    if program.len() < 6 {
        return Err(anyhow::anyhow!(
            "Program length is too short: expected at least 6, found {}",
            program.len()
        ));
    }

    Ok((
        program[3]
            .to_biguint()
            .try_into()
            .context("Failed to convert major version")?,
        program[4]
            .to_biguint()
            .try_into()
            .context("Failed to convert minor version")?,
        program[5]
            .to_biguint()
            .try_into()
            .context("Failed to convert patch version")?,
    ))
}

pub fn felt_str_to_fixed(felt_str: &str) -> anyhow::Result<String> {
    let felt = Felt::from_hex(felt_str)?;
    Ok(felt.to_fixed_hex_string())
}

pub async fn fetch_tx_block_number_from_voyager(
    chain_id: &ChainId,
    tx_hash: &str,
) -> anyhow::Result<u64> {
    let voyager_api_url = get_voyager_api_url(chain_id)
        .map(|url| url.to_string())
        .ok_or_else(|| anyhow::anyhow!("Invalid chain id"))?;
    let api_key = voyager_api_key().ok_or_else(|| anyhow::anyhow!("VOYAGER_API_KEY is not set"))?;
    let client = reqwest::Client::new();
    let url = format!("{}txns/{}", voyager_api_url, tx_hash);
    let call_result = client.get(&url).header("x-api-key", api_key).send().await;
    match call_result {
        Ok(response) => {
            if response.status().is_success() {
                let tx_details: Value = response.json().await.unwrap();

                match tx_details["blockNumber"].as_u64() {
                    Some(block_number) => Ok(block_number),
                    None => {
                        let timestamp = tx_details["timestamp"]
                            .as_u64()
                            .ok_or_else(|| anyhow::anyhow!("Block number not found"))?;
                        let rpc_client = create_rpc_client(chain_id);
                        let block_number = find_closest_block(&rpc_client, timestamp).await?;
                        Ok(block_number)
                    }
                }
            } else {
                Err(anyhow::anyhow!(
                    "Failed to fetch tx details from voyager api: {}",
                    response.status()
                ))
            }
        }
        Err(e) => Err(anyhow::anyhow!(
            "Failed to fetch tx details from voyager api: {}",
            e
        )),
    }
}

async fn find_closest_block(
    rpc_client: &JsonRpcClient<HttpTransport>,
    target_timestamp: u64,
) -> anyhow::Result<u64> {
    let mut low = 30000;
    let mut high = rpc_client.block_number().await?;

    while low <= high {
        let mid = (low + high) / 2;

        if let Ok(mid_timestamp) = get_block_timestamp(rpc_client, mid).await {
            if mid_timestamp >= target_timestamp && mid_timestamp <= target_timestamp + 60 {
                return Ok(mid);
            } else if mid_timestamp < target_timestamp {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        } else {
            // If block doesn't exist (out of range), adjust the bounds
            high = mid - 1;
        }
    }

    Err(anyhow::anyhow!("No block found"))
}

async fn get_block_timestamp(
    rpc_client: &JsonRpcClient<HttpTransport>,
    block_number: u64,
) -> anyhow::Result<u64> {
    let block = rpc_client
        .get_block_with_tx_hashes(BlockId::Number(block_number))
        .await?;
    match block {
        MaybePreConfirmedBlockWithTxHashes::Block(block) => Ok(block.timestamp),
        MaybePreConfirmedBlockWithTxHashes::PreConfirmedBlock(pending_block) => {
            Ok(pending_block.timestamp)
        }
    }
}

pub fn format_fee_payment(fee: &FeePayment) -> Option<String> {
    let amount = BigUint::from_bytes_be(&fee.amount.to_bytes_be());
    let decimals = BigUint::from(10u32).pow(18);
    let (whole, fraction) = amount.div_mod_floor(&decimals);

    let fraction_str = format!("{:018}", fraction); // uvek 18 decimala
    let unit_str = match fee.unit {
        PriceUnit::Wei => "ETH",
        PriceUnit::Fri => "STRK",
    };

    Some(format!("{}.{} {}", whole, fraction_str, unit_str))
}

#[cfg(test)]
mod deep_link_tests {
    use super::*;
    use starknet_api::core::ChainId;

    #[test]
    fn simulation_link_mainnet_omits_rpc_url() {
        let url = simulation_deep_link(
            "0x123",
            &["0x1".to_string(), "0x2".to_string()],
            1,
            &ChainId::Mainnet,
            Some(42),
            "https://rpc.example",
        );
        assert!(url.contains("chainId=SN_MAINNET"));
        assert!(url.contains("blockNumber=42"));
        assert!(!url.contains("rpcUrl="));
    }

    #[test]
    fn simulation_link_custom_chain_includes_rpc_url() {
        let url = simulation_deep_link(
            "0x123",
            &["0x1".to_string()],
            1,
            &ChainId::Other("my_chain".to_string()),
            None,
            "https://rpc.example",
        );
        assert!(url.contains("rpcUrl="));
        assert!(!url.contains("blockNumber="));
    }

    #[test]
    fn transaction_link_known_chain_vs_custom_rpc() {
        let known = transaction_deep_link(Some("SN_MAINNET"), None, "0xabc");
        assert!(known.contains("chainId=SN_MAINNET") && known.contains("txHash=0xabc"));

        let custom = transaction_deep_link(None, Some("https://rpc.example"), "0xabc");
        assert!(custom.contains("rpcUrl=") && custom.contains("txHash=0xabc"));
    }
}
