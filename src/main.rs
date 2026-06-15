use alloy::{
    network::Ethereum,
    primitives::{utils::format_ether, B256},
    providers::{Provider, RootProvider},
    signers::local::PrivateKeySigner,
};
use rand::rngs::OsRng;
use rand::RngCore;
use std::env;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use futures::stream::{self, StreamExt};
use tokio::sync::mpsc;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::time::{sleep, timeout, Duration};

type EthProvider = RootProvider<Ethereum>;

async fn check_one(provider: EthProvider) -> Result<Option<(String, String, String)>, Box<dyn Error + Send + Sync>> {
    let mut key_bytes = [0u8; 32];
    OsRng.fill_bytes(&mut key_bytes);

    let signer = match PrivateKeySigner::from_bytes(&B256::from(key_bytes)) {
        Ok(s) => s,
        Err(_) => return Ok(None),
    };

    let address = signer.address();
    let priv_hex = format!("0x{}", alloy::primitives::hex::encode(key_bytes));

    let balance = timeout(Duration::from_secs(15), provider.get_balance(address))
        .await
        .map_err(|_| "rpc timeout")??;

    if balance.is_zero() {
        return Ok(None);
    }

    let eth_balance = format_ether(balance);
    let addr_str = address.to_string();

    Ok(Some((priv_hex, addr_str, eth_balance)))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let rpc_urls_str = env::var("RPC_URLS")
        .ok()
        .or_else(|| env::var("RPC_URL").ok())
        .unwrap_or_else(|| "http://127.0.0.1:8545".to_string());

    let rpc_urls: Vec<String> = rpc_urls_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let mut provs: Vec<EthProvider> = Vec::new();
    for url in &rpc_urls {
        provs.push(RootProvider::new_http(url.parse()?));
    }
    let providers = Arc::new(provs);

    if providers.is_empty() {
        return Err("no rpc providers configured".into());
    }

    let workers: usize = env::var("WORKERS").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let per_worker: usize = env::var("PER_WORKER").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let total_env: Option<usize> = env::var("TOTAL").ok().and_then(|s| s.parse().ok());

    let total: usize = match (workers, per_worker, total_env) {
        (w, p, _) if w > 0 && p > 0 => w.saturating_mul(p),
        (_, _, Some(t)) => t,
        _ => 1024,
    };

    let concurrency: usize = env::var("CONCURRENCY").ok().and_then(|s| s.parse().ok()).unwrap_or(256);

    let save_path = env::var("SAVE_FILE").unwrap_or_else(|_| "found_keys.txt".to_string());
    let (tx, mut rx) = mpsc::channel::<String>(4096);

    let save_path_clone = save_path.clone();
    let writer = tokio::spawn(async move {
        const BATCH_SIZE: usize = 64;
        const FLUSH_INTERVAL_MS: u64 = 500;

        let mut file = match OpenOptions::new().create(true).append(true).open(&save_path_clone).await {
            Ok(f) => f,
            Err(e) => {
                eprintln!("failed to open save file {}: {}", save_path_clone, e);
                return;
            }
        };

        let mut buffer: Vec<String> = Vec::with_capacity(BATCH_SIZE);
        loop {
            match rx.recv().await {
                Some(k) => buffer.push(k),
                None => {
                    if !buffer.is_empty() {
                        if let Err(e) = file.write_all((buffer.join("\n") + "\n").as_bytes()).await {
                            eprintln!("writer: failed to write final batch: {}", e);
                        }
                        let _ = file.flush().await;
                    }
                    break;
                }
            }

            let start = tokio::time::Instant::now();
            while buffer.len() < BATCH_SIZE {
                if let Ok(item) = rx.try_recv() {
                    buffer.push(item);
                } else {
                    if start.elapsed() >= Duration::from_millis(FLUSH_INTERVAL_MS) {
                        break;
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            }

            if !buffer.is_empty() {
                if let Err(e) = file.write_all((buffer.join("\n") + "\n").as_bytes()).await {
                    eprintln!("writer: failed to write batch: {}", e);
                }
                if let Err(e) = file.flush().await {
                    eprintln!("writer: failed to flush: {}", e);
                }
                buffer.clear();
            }
        }
    });

    println!("RPC(s)={} total={} concurrency={} save_file={}", rpc_urls.join(","), total, concurrency, save_path);

    let counter = Arc::new(AtomicUsize::new(0));
    let checked = Arc::new(AtomicUsize::new(0));

    let providers_clone = providers.clone();
    let counter_clone = counter.clone();
    let s = stream::iter(0..total).map(move |_| {
        let provs = providers_clone.clone();
        let ctr = counter_clone.clone();
        async move {
            let idx = ctr.fetch_add(1, Ordering::Relaxed);
            let provider = provs[idx % provs.len()].clone();
            check_one(provider).await
        }
    });

    let mut results = s.buffer_unordered(concurrency);
    let log_interval = (total / 100).max(1000);

    while let Some(res) = results.next().await {
        let n = checked.fetch_add(1, Ordering::Relaxed) + 1;
        if n % log_interval == 0 {
            println!("progress: {}/{}", n, total);
        }
        match res {
            Ok(Some((priv_hex, addr_str, bal))) => {
                println!("FOUND {} -> {} ETH", addr_str, bal);
                let entry = format!("{} {} {} ETH", priv_hex, addr_str, bal);
                if let Err(e) = tx.send(entry).await {
                    eprintln!("failed to send key to writer: {}", e);
                }
            }
            Ok(None) => {}
            Err(e) => eprintln!("check_one failed: {}", e),
        }
    }

    drop(tx);
    if let Err(e) = writer.await {
        eprintln!("writer task failed: {}", e);
    }

    Ok(())
}
