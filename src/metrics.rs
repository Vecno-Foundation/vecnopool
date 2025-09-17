use prometheus::{
    CounterVec, Gauge, register_counter_vec, register_gauge, Counter, register_counter,
    Histogram, register_histogram,
    Encoder, TextEncoder,
};
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

    pub static ref SHARES_PURGED: Counter = register_counter!(
        "shares_purged_total",
        "Total number of shares purged from the database"
    ).unwrap();

    pub static ref BLOCKS_PURGED: Counter = register_counter!(
        "blocks_purged_total",
        "Total number of processed blocks purged from the database"
    ).unwrap();

    pub static ref SHARE_VALIDATION_LATENCY: Histogram = register_histogram!(
        "share_validation_latency_seconds",
        "Time to validate a share",
        vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]
    ).unwrap();

    pub static ref DIFFICULTY_ADJUSTMENTS: CounterVec = register_counter_vec!(
        "difficulty_adjustments_total",
        "Total number of difficulty adjustments by miner",
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

async fn metrics_handler(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path();

    if path == "/metrics" {
        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer).unwrap();
        Ok(Response::builder()
            .header("Content-Type", "text/plain; version=0.0.4")
            .body(Body::from(buffer))
            .unwrap())
    } else {
        let metric_families = prometheus::gather();
        let html = generate_metrics_html(&metric_families);
        Ok(Response::builder()
            .header("Content-Type", "text/html")
            .body(Body::from(html))
            .unwrap())
    }
}

fn generate_metrics_html(metric_families: &[prometheus::proto::MetricFamily]) -> String {
    let mut html = String::from(
        r#"
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Mining Pool Metrics</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f4f4f9;
            color: #333;
        }
        h1 {
            text-align: center;
            color: #2c3e50;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            background-color: #fff;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #3498db;
            color: white;
        }
        tr:hover {
            background-color: #f1f1f1;
        }
        .metric-name {
            font-weight: bold;
            color: #2c3e50;
        }
        .metric-help {
            font-style: italic;
            color: #7f8c8d;
        }
        .metric-labels {
            font-size: 0.9em;
            color: #34495e;
        }
        footer {
            text-align: center;
            margin-top: 20px;
            color: #7f8c8d;
        }
        a {
            color: #3498db;
            text-decoration: none;
        }
        a:hover {
            text-decoration: underline;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Mining Pool Metrics</h1>
        <table>
            <tr>
                <th>Metric Name</th>
                <th>Description</th>
                <th>Labels</th>
                <th>Value</th>
                <th>Type</th>
            </tr>
"#,
    );

    for family in metric_families {
        let name = family.get_name();
        let help = family.get_help();
        let metric_type = match family.get_field_type() {
            prometheus::proto::MetricType::COUNTER => "Counter",
            prometheus::proto::MetricType::GAUGE => "Gauge",
            prometheus::proto::MetricType::HISTOGRAM => "Histogram",
            prometheus::proto::MetricType::SUMMARY => "Summary",
            _ => "Unknown",
        };

        for metric in family.get_metric() {
            let labels = metric
                .get_label()
                .iter()
                .map(|l| format!("{}=\"{}\"", l.get_name(), l.get_value()))
                .collect::<Vec<String>>()
                .join(", ");
            
            let value = match family.get_field_type() {
                prometheus::proto::MetricType::COUNTER => {
                    format!("{:.2}", metric.get_counter().get_value())
                }
                prometheus::proto::MetricType::GAUGE => {
                    format!("{:.2}", metric.get_gauge().get_value())
                }
                prometheus::proto::MetricType::HISTOGRAM => {
                    let hist = metric.get_histogram();
                    format!(
                        "count: {}, sum: {:.2}s, buckets: [{}]",
                        hist.get_sample_count(),
                        hist.get_sample_sum(),
                        hist.get_bucket()
                            .iter()
                            .map(|b| format!("≤{}s: {}", b.get_upper_bound(), b.get_cumulative_count()))
                            .collect::<Vec<String>>()
                            .join(", ")
                    )
                }
                _ => "N/A".to_string(),
            };

            html.push_str(&format!(
                r#"
                <tr>
                    <td class="metric-name">{}</td>
                    <td class="metric-help">{}</td>
                    <td class="metric-labels">{}</td>
                    <td>{}</td>
                    <td>{}</td>
                </tr>
                "#,
                name, help, labels, value, metric_type
            ));
        }
    }

    html.push_str(
        r#"
        </table>
        <footer>
            <p>Visit <a href="/metrics">/metrics</a> for raw Prometheus metrics.</p>
        </footer>
    </div>
</body>
</html>
"#,
    );

    html
}