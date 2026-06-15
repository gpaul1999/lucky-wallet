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
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};

type EthProvider = RootProvider<Ethereum>;

// Returns None on zero balance, Some(...) on hit. Only allocates strings on a hit.
async fn check_one(provider: EthProvider) -> Result<Option<(String, String, String)>, Box<dyn Error + Send + Sync>> {
    let mut key_bytes = [0u8; 32];
    OsRng.fill_bytes(&mut key_bytes);

    let signer = match PrivateKeySigner::from_bytes(&B256::from(key_bytes)) {
        Ok(s) => s,
        Err(_) => return Ok(None),
    };

    let address = signer.address();

    let balance = timeout(Duration::from_secs(15), provider.get_balance(address))
        .await
        .map_err(|_| "rpc timeout")??;

    if balance.is_zero() {
        return Ok(None);
    }

    let priv_hex = format!("0x{}", alloy::primitives::hex::encode(key_bytes));
    let addr_str = address.to_string();
    let eth_balance = format_ether(balance);

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

    let concurrency: usize = env::var("CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let save_path = env::var("SAVE_FILE").unwrap_or_else(|_| "found_keys.txt".to_string());
    let checked = Arc::new(AtomicUsize::new(0));

    // Channel to writer task — only fires on a hit, so buffer can be small
    let (tx, mut rx) = mpsc::channel::<String>(256);

    // Writer task: batches found keys and flushes to disk
    let save_path_clone = save_path.clone();
    let writer = tokio::spawn(async move {
        let mut file = match OpenOptions::new().create(true).append(true).open(&save_path_clone).await {
            Ok(f) => f,
            Err(e) => { eprintln!("cannot open save file: {}", e); return; }
        };

        while let Some(entry) = rx.recv().await {
            if let Err(e) = file.write_all(format!("{}\n", entry).as_bytes()).await {
                eprintln!("write error: {}", e);
            }
            // Flush immediately — hits are rare, latency doesn't matter
            let _ = file.flush().await;
        }
    });

    // Stats task: prints throughput every 10 seconds
    let stats_checked = checked.clone();
    tokio::spawn(async move {
        let mut last = 0usize;
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.tick().await; // skip the immediate first tick
        loop {
            interval.tick().await;
            let now = stats_checked.load(Ordering::Relaxed);
            let rate = (now - last) / 10;
            println!("checked: {:>12} | {:>6}/s", now, rate);
            last = now;
        }
    });

    println!("RPC(s)    : {}", rpc_urls.join(", "));
    println!("workers   : {}", concurrency);
    println!("save file : {}", save_path);
    println!("running forever — Ctrl+C to stop\n");

    // Spawn N worker tasks, each pinned to a provider (round-robin)
    let mut handles = Vec::with_capacity(concurrency);
    for i in 0..concurrency {
        let provider = providers[i % providers.len()].clone();
        let tx = tx.clone();
        let checked = checked.clone();

        handles.push(tokio::spawn(async move {
            let mut consecutive_errors: u32 = 0;

            loop {
                match check_one(provider.clone()).await {
                    Ok(Some((priv_hex, addr_str, bal))) => {
                        consecutive_errors = 0;
                        checked.fetch_add(1, Ordering::Relaxed);
                        println!("*** FOUND {} -> {} ETH ***", addr_str, bal);
                        let entry = format!("{} {} {} ETH", priv_hex, addr_str, bal);
                        if tx.send(entry).await.is_err() {
                            break; // channel closed → shutdown in progress
                        }
                    }
                    Ok(None) => {
                        consecutive_errors = 0;
                        checked.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        if consecutive_errors >= 3 {
                            let wait_secs = consecutive_errors.min(60) as u64;
                            eprintln!("worker {}: rpc error ({}): {} — backing off {}s", i, consecutive_errors, e, wait_secs);
                            sleep(Duration::from_secs(wait_secs)).await;
                        }
                    }
                }
            }
        }));
    }

    // Block until Ctrl+C
    tokio::signal::ctrl_c().await?;
    let total = checked.load(Ordering::Relaxed);
    println!("\nCtrl+C received — shutting down (total checked: {})", total);

    for h in &handles {
        h.abort();
    }

    drop(tx);
    let _ = writer.await;

    Ok(())
}
