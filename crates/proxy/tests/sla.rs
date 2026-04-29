mod support;

use http::StatusCode;
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run manually because latency budgets depend on the local machine"]
async fn local_proxy_p95_latency_stays_within_sla_budget() {
    let topology = support::spawn_topology().await;
    let requests = support::env_usize("DXGATE_SLA_REQUESTS", 100);
    let p95_budget_ms = support::env_usize("DXGATE_SLA_P95_MS", 100);
    let mut latencies = Vec::with_capacity(requests);

    for i in 0..requests {
        let started = Instant::now();
        let (status, body) = support::get_text(topology.proxy_addr, &format!("/sla/{i}")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("/sla/"));
        latencies.push(started.elapsed());
    }

    let mut p50_latencies = latencies.clone();
    let mut p99_latencies = latencies.clone();
    let p50 = support::percentile(&mut p50_latencies, 50);
    let p95 = support::percentile(&mut latencies, 95);
    let p99 = support::percentile(&mut p99_latencies, 99);
    println!(
        "SLA result: requests={requests}, p50={p50:?}, p95={p95:?}, p99={p99:?}, p95_budget_ms={p95_budget_ms}"
    );
    assert!(
        p95 <= Duration::from_millis(p95_budget_ms as u64),
        "p95 latency {p95:?} exceeded SLA budget {p95_budget_ms}ms"
    );
}
