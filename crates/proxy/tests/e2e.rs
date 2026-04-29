mod support;

use http::StatusCode;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn proxy_forwards_http_request_to_configured_backend() {
    let topology = support::spawn_topology().await;

    let (status, body) = support::get_text(topology.proxy_addr, "/orders/42?debug=true").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "kgate example backend path=/orders/42?debug=true");
}
