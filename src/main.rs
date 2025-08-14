use ethers::prelude::*;
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
use tokio::time::{sleep, Duration};

async fn check_one(provider: Arc<Provider<Http>>) -> Result<Option<(String, Address, String)>, Box<dyn Error + Send + Sync>> {
    // Generate a new random 32-byte private key
    let mut key = [0u8; 32];
    OsRng.fill_bytes(&mut key);
    let priv_hex = format!("0x{}", hex::encode(key));

    // Parse into a LocalWallet
    let wallet: LocalWallet = priv_hex.parse()?;
    let address = wallet.address();

    // Fetch balance
    let balance = provider.get_balance(address, None).await?;
    if balance.is_zero() {
        return Ok(None);
    }
    let eth_balance = ethers::utils::format_ether(balance);
    Ok(Some((priv_hex, address, eth_balance)))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Read RPC URLs: prefer RPC_URLS (comma-separated), fall back to RPC_URL, default to local node
    let rpc_urls_str = env::var("RPC_URLS").ok().or_else(|| env::var("RPC_URL").ok()).unwrap_or_else(|| "http://127.0.0.1:8545".to_string());
    let rpc_urls: Vec<String> = rpc_urls_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // Build provider pool
    let mut provs = Vec::new();
    for url in &rpc_urls {
        provs.push(Provider::<Http>::try_from(url.as_str())?);
    }
    let providers = Arc::new(provs);
    if providers.is_empty() {
        return Err("no rpc providers configured".into());
    }

    // Configuration: total work and concurrency
    let workers: usize = env::var("WORKERS").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let per_worker: usize = env::var("PER_WORKER").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let total_env: Option<usize> = env::var("TOTAL").ok().and_then(|s| s.parse().ok());

    let total: usize = match (workers, per_worker, total_env) {
        (w, p, _) if w > 0 && p > 0 => w.saturating_mul(p),
        (_, _, Some(t)) => t,
        _ => 1024,
    };

    let concurrency: usize = env::var("CONCURRENCY").ok().and_then(|s| s.parse().ok()).unwrap_or(256);

    // Save file and writer channel
    let save_path = env::var("SAVE_FILE").unwrap_or_else(|_| "found_keys.txt".to_string());
    let (tx, mut rx) = mpsc::channel::<String>(4096);

    // Spawn writer task: batch writes and flush periodically
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
            // wait for at least one item, or exit if channel closed
            match rx.recv().await {
                Some(k) => buffer.push(k),
                None => {
                    // channel closed, flush remaining
                    if !buffer.is_empty() {
                        if let Err(e) = file.write_all((buffer.join("\n") + "\n").as_bytes()).await {
                            eprintln!("writer: failed to write final batch: {}", e);
                        }
                        let _ = file.flush().await;
                    }
                    break;
                }
            }

            // try to collect a small batch or wait a short time
            let start = tokio::time::Instant::now();
            while buffer.len() < BATCH_SIZE {
                if let Ok(item) = rx.try_recv() {
                    buffer.push(item);
                } else {
                    // brief wait to allow more items to accumulate
                    if start.elapsed() >= Duration::from_millis(FLUSH_INTERVAL_MS) {
                        break;
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            }

            // write batch
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

    // Atomic counter for round-robin provider selection
    let counter = Arc::new(AtomicUsize::new(0));

    // Create a stream of tasks and run with a bounded concurrency
    let providers_clone = providers.clone();
    let counter_clone = counter.clone();
    let s = stream::iter(0..total).map(move |_| {
        let provs = providers_clone.clone();
        let ctr = counter_clone.clone();
        async move {
            let idx = ctr.fetch_add(1, Ordering::Relaxed);
            let provider = provs[idx % provs.len()].clone();
            check_one(Arc::new(provider)).await
        }
    });

    let mut results = s.buffer_unordered(concurrency);

    while let Some(res) = results.next().await {
        match res {
            Ok(Some((priv_hex, address, bal))) => {
                let addr_str = format!("0x{}", hex::encode(address));
                println!("{} -> {} ETH", addr_str, bal);
                if let Err(e) = tx.send(priv_hex).await {
                    eprintln!("failed to send key to writer: {}", e);
                }
            }
            Ok(None) => { /* zero balance, skip */ }
            Err(e) => eprintln!("check_one failed: {}", e),
        }
    }

    // Close writer and wait for it to finish
    drop(tx);
    if let Err(e) = writer.await {
        eprintln!("writer task failed: {}", e);
    }

    Ok(())
}
