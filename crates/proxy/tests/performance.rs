mod support;

use std::time::Instant;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run manually because throughput depends on the local machine"]
async fn local_proxy_sustains_minimum_throughput() {
    let topology = support::spawn_topology().await;
    let requests = support::env_usize("KGATE_PERF_REQUESTS", 200);
    let minimum_rps = support::env_usize("KGATE_PERF_MIN_RPS", 500);

    let started = Instant::now();
    let mut latencies = support::run_concurrent_requests(topology.proxy_addr, requests).await;
    let elapsed = started.elapsed();
    let rps = requests as f64 / elapsed.as_secs_f64();
    let mut p50_latencies = latencies.clone();
    let mut p99_latencies = latencies.clone();
    let p50 = support::percentile(&mut p50_latencies, 50);
    let p95 = support::percentile(&mut latencies, 95);
    let p99 = support::percentile(&mut p99_latencies, 99);
    println!(
        "Performance result: requests={requests}, elapsed={elapsed:?}, rps={rps:.2}, p50={p50:?}, p95={p95:?}, p99={p99:?}, minimum_rps={minimum_rps}"
    );

    assert!(
        rps >= minimum_rps as f64,
        "throughput {rps:.2} rps below minimum {minimum_rps} rps; p95 latency {p95:?}"
    );
}
