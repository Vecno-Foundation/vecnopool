use prometheus::{CounterVec, Gauge, register_counter_vec, register_gauge, Encoder, TextEncoder};
use hyper::{Body, Request, Response, Server};
use hyper::service::{make_service_fn, service_fn};
use std::convert::Infallible;
use std::net::SocketAddr;
use log::error;

lazy_static::lazy_static! {
    pub static ref TOTAL_SHARES_RECORDED: CounterVec = register_counter_vec!(
        "total_shares_recorded",
        "Total number of shares recorded by address",
        &["address"]
    ).unwrap();

    pub static ref SHARE_WINDOW_SIZE: Gauge = register_gauge!(
        "share_window_size",
        "Current size of the share window"
    ).unwrap();

    pub static ref DB_QUERIES_SUCCESS: CounterVec = register_counter_vec!(
        "db_queries_success",
        "Number of successful database queries",
        &["query_type"]
    ).unwrap();

    pub static ref DB_QUERIES_FAILED: CounterVec = register_counter_vec!(
        "db_queries_failed",
        "Number of failed database queries",
        &["query_type"]
    ).unwrap();

    pub static ref MINER_ADDED_SHARES: CounterVec = register_counter_vec!(
        "miner_added_shares",
        "Number of shares added by miner",
        &["address"]
    ).unwrap();

    pub static ref MINER_INVALID_SHARES: CounterVec = register_counter_vec!(
        "miner_invalid_shares",
        "Number of invalid shares submitted by miner",
        &["address"]
    ).unwrap();

    pub static ref MINER_DUPLICATED_SHARES: CounterVec = register_counter_vec!(
        "miner_duplicated_shares",
        "Number of duplicated shares submitted by miner",
        &["address"]
    ).unwrap();

    pub static ref MINER_ADDRESS_MISMATCH: CounterVec = register_counter_vec!(
        "miner_address_mismatch",
        "Number of address mismatch errors by miner",
        &["address"]
    ).unwrap();

    pub static ref BLOCK_DETAILS_FETCH_SUCCESS: CounterVec = register_counter_vec!(
        "block_details_fetch_success",
        "Number of successful block details fetches",
        &["block_hash"]
    ).unwrap();

    pub static ref BLOCK_DETAILS_FETCH_FAILED: CounterVec = register_counter_vec!(
        "block_details_fetch_failed",
        "Number of failed block details fetches",
        &["block_hash"]
    ).unwrap();

    pub static ref BLOCK_REWARDS: CounterVec = register_counter_vec!(
        "block_rewards",
        "Total block rewards by pool address",
        &["pool_address"]
    ).unwrap();

    pub static ref REWARDS_DISTRIBUTED: CounterVec = register_counter_vec!(
        "rewards_distributed",
        "Amount of rewards distributed to miners",
        &["address", "block_hash"]
    ).unwrap();

    pub static ref SHARE_PROCESSING_FAILED: CounterVec = register_counter_vec!(
        "share_processing_failed",
        "Number of failed share processing attempts",
        &["address"]
    ).unwrap();

    pub static ref TRANSACTION_CREATION_FAILED: CounterVec = register_counter_vec!(
        "transaction_creation_failed",
        "Number of failed transaction creations",
        &["address"]
    ).unwrap();

    pub static ref TRANSACTION_SUBMISSION_FAILED: CounterVec = register_counter_vec!(
        "transaction_submission_failed",
        "Number of failed transaction submissions",
        &["address"]
    ).unwrap();
}

pub async fn start_metrics_server() {
    let addr = SocketAddr::from(([0, 0, 0, 0], 9090));
    let make_service = make_service_fn(|_conn| async {
        Ok::<_, Infallible>(service_fn(metrics_handler))
    });

    let server = Server::bind(&addr).serve(make_service);

    if let Err(e) = server.await {
        error!("Metrics server error: {}", e);
    }
}

async fn metrics_handler(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();

    Ok(Response::new(Body::from(buffer)))
}