use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use dxgate_proxy::ProxyState;
use serde::Serialize;
use std::net::SocketAddr;

#[derive(Debug, Clone, Serialize)]
pub struct BuildInfo {
    pub name: &'static str,
    pub version: &'static str,
}

#[derive(Clone)]
pub struct AdminServer {
    state: ProxyState,
    build: BuildInfo,
    proxy_port: u16,
}

impl AdminServer {
    pub fn new(state: ProxyState, proxy_addr: SocketAddr) -> Self {
        Self {
            state,
            build: BuildInfo {
                name: "dxgate",
                version: env!("CARGO_PKG_VERSION"),
            },
            proxy_port: proxy_addr.port(),
        }
    }

    pub async fn serve(self, addr: SocketAddr) -> std::io::Result<()> {
        let app = Router::new()
            .route("/", get(admin_ui))
            .route("/ui", get(admin_ui))
            .route("/assets/dxgate-logo.svg", get(logo_svg))
            .route("/healthz", get(healthz))
            .route("/readyz", get(readyz))
            .route("/metrics", get(metrics))
            .route("/debug/config", get(debug_config))
            .route("/debug/routes", get(debug_routes))
            .route("/debug/clusters", get(debug_clusters))
            .route("/debug/backends", get(debug_backends))
            .route("/debug/policies", get(debug_policies))
            .with_state(self);

        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .map_err(std::io::Error::other)
    }
}

async fn admin_ui(State(admin): State<AdminServer>) -> Html<String> {
    Html(admin_html(admin.proxy_port))
}

async fn logo_svg() -> Response {
    (
        [(header::CONTENT_TYPE, "image/svg+xml; charset=utf-8")],
        include_str!("../../../logo/dxgate-logo.svg"),
    )
        .into_response()
}

async fn healthz(State(admin): State<AdminServer>) -> Json<BuildInfo> {
    Json(admin.build)
}

async fn readyz(State(admin): State<AdminServer>) -> Response {
    let readiness = admin.state.readiness().await;
    let status = if readiness.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(readiness)).into_response()
}

async fn metrics(State(admin): State<AdminServer>) -> String {
    let readiness = admin.state.readiness().await;
    let proxy = admin.state.metrics();
    let mut out = format!(
        "# HELP dxgate_ready Whether dxgate has accepted runtime config\n# TYPE dxgate_ready gauge\ndxgate_ready {}\n# HELP dxgate_config_conflicts Current rejected config conflicts\n# TYPE dxgate_config_conflicts gauge\ndxgate_config_conflicts {}\n",
        if readiness.ready { 1 } else { 0 },
        readiness.conflicts.len()
    );
    out.push_str("# HELP dxgate_requests_total Total requests observed by dxgate\n# TYPE dxgate_requests_total counter\n");
    out.push_str(&format!("dxgate_requests_total {}\n", proxy.total_requests));
    out.push_str("# HELP dxgate_agent_requests_total Agent protocol requests observed by dxgate\n# TYPE dxgate_agent_requests_total counter\n");
    out.push_str(&format!(
        "dxgate_agent_requests_total {}\n",
        proxy.agent_requests
    ));
    out.push_str("# HELP dxgate_policy_denied_total Requests denied by dxgate policy\n# TYPE dxgate_policy_denied_total counter\n");
    out.push_str(&format!(
        "dxgate_policy_denied_total {}\n",
        proxy.policy_denied
    ));
    out.push_str("# HELP dxgate_upstream_failures_total Upstream failures observed by dxgate\n# TYPE dxgate_upstream_failures_total counter\n");
    out.push_str(&format!(
        "dxgate_upstream_failures_total {}\n",
        proxy.upstream_failures
    ));
    for route in proxy.routes {
        out.push_str(&format!(
            "dxgate_agent_route_requests_total{{protocol=\"{}\",route=\"{}\",backend=\"{}\"}} {}\n",
            route.protocol, route.route, route.backend, route.requests
        ));
        out.push_str(&format!(
            "dxgate_agent_route_failures_total{{protocol=\"{}\",route=\"{}\",backend=\"{}\"}} {}\n",
            route.protocol, route.route, route.backend, route.failures
        ));
        out.push_str(&format!(
            "dxgate_agent_route_latency_ms_sum{{protocol=\"{}\",route=\"{}\",backend=\"{}\"}} {}\n",
            route.protocol, route.route, route.backend, route.latency_ms_sum
        ));
    }
    out
}

async fn debug_config(State(admin): State<AdminServer>) -> Json<dxgate_core::RuntimeConfig> {
    Json(admin.state.config().await)
}

async fn debug_routes(State(admin): State<AdminServer>) -> Json<serde_json::Value> {
    let cfg = admin.state.config().await;
    let routes: Vec<_> = cfg
        .listeners
        .iter()
        .flat_map(|listener| {
            listener.virtual_hosts.iter().flat_map(move |host| {
                host.routes.iter().map(move |route| {
                    serde_json::json!({
                        "listener": listener.name,
                        "virtualHost": host.name,
                        "route": route.name,
                        "domains": host.domains,
                        "weightedClusters": route.weighted_clusters,
                    })
                })
            })
        })
        .collect();
    Json(serde_json::json!(routes))
}

async fn debug_clusters(State(admin): State<AdminServer>) -> Json<serde_json::Value> {
    let cfg = admin.state.config().await;
    Json(serde_json::json!(cfg.clusters))
}

async fn debug_backends(State(admin): State<AdminServer>) -> Json<serde_json::Value> {
    let cfg = admin.state.config().await;
    Json(serde_json::json!({
        "providers": cfg.providers,
        "backends": cfg.backends,
        "routes": cfg.routes,
    }))
}

async fn debug_policies(State(admin): State<AdminServer>) -> Json<serde_json::Value> {
    let cfg = admin.state.config().await;
    Json(serde_json::json!(cfg.policies))
}

fn admin_html(proxy_port: u16) -> String {
    ADMIN_HTML.replace("__DXGATE_PROXY_PORT__", &proxy_port.to_string())
}

const ADMIN_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="icon" href="data:,">
  <title>dxgate admin</title>
  <style>
    :root {
      --ink: #171a1f;
      --muted: #667085;
      --line: #d9dde5;
      --panel: #ffffff;
      --page: #f5f6f8;
      --soft: #eceff4;
      --teal: #097969;
      --blue: #2457a6;
      --amber: #9a5b00;
      --red: #b42318;
      --radius: 8px;
      color-scheme: light;
      font-family: ui-sans-serif, "Aptos", "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      background: var(--page);
      color: var(--ink);
      letter-spacing: 0;
    }
    button, input, textarea, select { font: inherit; }
    .shell {
      display: grid;
      grid-template-columns: 216px 1fr;
      min-height: 100vh;
    }
    .nav {
      border-right: 1px solid var(--line);
      background: #fbfbfc;
      padding: 18px 14px;
      position: sticky;
      top: 0;
      height: 100vh;
    }
    .brand {
      display: flex;
      align-items: center;
      padding: 3px 4px 18px;
      border-bottom: 1px solid var(--line);
      margin-bottom: 14px;
    }
    .brand-logo {
      display: block;
      width: 126px;
      height: auto;
    }
    .nav button {
      width: 100%;
      height: 36px;
      border: 1px solid transparent;
      background: transparent;
      border-radius: 6px;
      display: flex;
      align-items: center;
      gap: 9px;
      color: #313946;
      cursor: pointer;
      padding: 0 10px;
      margin: 2px 0;
      text-align: left;
    }
    .nav button:hover { background: var(--soft); }
    .nav button.active { background: #e4ebe9; border-color: #bfd5ce; color: #063f37; }
    main { padding: 18px 22px 30px; min-width: 0; }
    .topbar {
      min-height: 54px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      border-bottom: 1px solid var(--line);
      margin-bottom: 18px;
    }
    h1 { font-size: 22px; margin: 0; font-weight: 760; }
    .statusline { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    .pill {
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: 999px;
      padding: 5px 9px;
      font-size: 12px;
      color: #344054;
      white-space: nowrap;
    }
    .pill.ok { border-color: #a4d1c4; color: #065f46; background: #eefaf6; }
    .pill.bad { border-color: #f2b8b5; color: var(--red); background: #fff1f0; }
    .actions { display: flex; gap: 8px; align-items: center; }
    .btn {
      border: 1px solid #bac2cf;
      border-radius: 6px;
      height: 34px;
      padding: 0 11px;
      background: #fff;
      cursor: pointer;
      color: #1f2937;
    }
    .btn:hover { border-color: #7c8798; background: #f7f8fa; }
    .btn.primary { background: var(--ink); color: #fff; border-color: var(--ink); }
    .grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(140px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }
    .metric, .panel {
      background: rgba(255,255,255,.92);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: 0 1px 2px rgba(16,24,40,.04);
    }
    .metric { padding: 13px 14px; min-height: 86px; }
    .metric span { color: var(--muted); font-size: 12px; }
    .metric strong { display: block; font-size: 27px; margin-top: 8px; }
    .panel { margin-top: 12px; overflow: hidden; }
    .panel h2 {
      margin: 0;
      padding: 12px 14px;
      font-size: 14px;
      border-bottom: 1px solid var(--line);
      background: #fbfbfc;
    }
    .table-wrap { overflow: auto; }
    table { border-collapse: collapse; width: 100%; min-width: 760px; }
    th, td { border-bottom: 1px solid var(--line); padding: 10px 12px; text-align: left; vertical-align: top; }
    th { color: var(--muted); font-size: 12px; font-weight: 680; background: #fbfbfc; }
    td { font-size: 13px; }
    code, pre, textarea {
      font-family: ui-monospace, "SFMono-Regular", "Cascadia Mono", monospace;
      letter-spacing: 0;
    }
    pre {
      margin: 0;
      padding: 12px;
      overflow: auto;
      max-height: 520px;
      background: #111827;
      color: #e5e7eb;
      font-size: 12px;
    }
    .muted { color: var(--muted); }
    .tag {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      border: 1px solid var(--line);
      padding: 2px 7px;
      font-size: 12px;
      margin: 0 4px 4px 0;
      background: #fff;
    }
    .tag.llm { border-color: #c7d7fb; color: var(--blue); background: #f2f6ff; }
    .tag.mcp { border-color: #a4d1c4; color: var(--teal); background: #eefaf6; }
    .tag.a2a { border-color: #f4c47f; color: var(--amber); background: #fff7e8; }
    .split { display: grid; grid-template-columns: minmax(280px, 420px) 1fr; gap: 12px; }
    .form { padding: 12px; display: grid; gap: 10px; }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 12px; }
    input, textarea, select {
      width: 100%;
      border: 1px solid #bfc6d1;
      border-radius: 6px;
      background: #fff;
      color: var(--ink);
      padding: 8px 9px;
    }
    textarea { min-height: 170px; resize: vertical; }
    .error {
      color: var(--red);
      background: #fff1f0;
      border: 1px solid #f2b8b5;
      padding: 10px 12px;
      border-radius: 6px;
      margin: 10px 0;
    }
    .hidden { display: none !important; }
    @media (max-width: 980px) {
      .shell { grid-template-columns: 1fr; }
      .nav { position: static; height: auto; border-right: 0; border-bottom: 1px solid var(--line); }
      .grid { grid-template-columns: repeat(2, 1fr); }
      .split { grid-template-columns: 1fr; }
    }
    @media (max-width: 560px) {
      main { padding: 14px; }
      .topbar { align-items: flex-start; flex-direction: column; }
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <aside class="nav">
      <div class="brand"><img class="brand-logo" src="/assets/dxgate-logo.svg" alt="dxgate logo"></div>
      <button class="active" data-tab="overview" data-title="Overview">Overview</button>
      <button data-tab="routes" data-title="Routes">Routes</button>
      <button data-tab="backends" data-title="Backends">Backends</button>
      <button data-tab="policies" data-title="Policies">Policies</button>
      <button data-tab="playground" data-title="MCP">MCP</button>
      <button data-tab="config" data-title="Config">Config</button>
    </aside>
    <main>
      <div class="topbar">
        <div>
          <h1 id="title">Overview</h1>
          <div class="statusline" id="statusline"></div>
          <div class="muted source-line" id="source-line">loading runtime data</div>
        </div>
        <div class="actions">
          <button class="btn" id="copy-config">Copy config</button>
          <button class="btn primary" id="refresh">Refresh</button>
        </div>
      </div>
      <div id="error" class="error hidden"></div>

      <section id="tab-overview">
        <div class="grid">
          <div class="metric"><span>Ready</span><strong id="metric-ready">loading</strong></div>
          <div class="metric"><span>Agent routes</span><strong id="metric-routes">loading</strong></div>
          <div class="metric"><span>Backends</span><strong id="metric-backends">loading</strong></div>
          <div class="metric"><span>Policy denies</span><strong id="metric-denies">loading</strong></div>
        </div>
        <div class="panel">
          <h2>Route traffic</h2>
          <div class="table-wrap"><table id="traffic-table"></table></div>
        </div>
      </section>

      <section id="tab-routes" class="hidden">
        <div class="panel"><h2>Agent routes</h2><div class="table-wrap"><table id="routes-table"></table></div></div>
        <div class="panel"><h2>Dubbo routes</h2><div class="table-wrap"><table id="dubbo-routes-table"></table></div></div>
      </section>

      <section id="tab-backends" class="hidden">
        <div class="panel"><h2>Backends</h2><div class="table-wrap"><table id="backends-table"></table></div></div>
        <div class="panel"><h2>Providers</h2><div class="table-wrap"><table id="providers-table"></table></div></div>
      </section>

      <section id="tab-policies" class="hidden">
        <div class="panel"><h2>Policies</h2><div class="table-wrap"><table id="policies-table"></table></div></div>
      </section>

      <section id="tab-playground" class="hidden">
        <div class="split">
          <div class="panel">
            <h2>MCP request</h2>
            <div class="form">
              <label>Proxy base<input id="mcp-base"></label>
              <label>Path<input id="mcp-path"></label>
              <label>Body<textarea id="mcp-body"></textarea></label>
              <button class="btn primary" id="mcp-send">Send</button>
            </div>
          </div>
          <div class="panel"><h2>Response</h2><pre id="mcp-result"></pre></div>
        </div>
      </section>

      <section id="tab-config" class="hidden">
        <div class="panel"><h2>Effective runtime config</h2><pre id="config-json">loading</pre></div>
      </section>
    </main>
  </div>
  <script>
    const proxyPort = __DXGATE_PROXY_PORT__;
    const state = { ready: null, config: null, metrics: '', loadedAt: null };
    const $ = (id) => document.getElementById(id);

    function proxyBase() {
      const port = proxyPort === 80 ? '' : ':' + proxyPort;
      return location.protocol + '//' + location.hostname + port;
    }

    function showError(message) {
      $('error').textContent = message;
      $('error').classList.toggle('hidden', !message);
    }

    async function getJson(path) {
      const response = await fetch(path, { cache: 'no-store' });
      if (!response.ok) throw new Error(path + ' returned ' + response.status);
      return response.json();
    }

    async function refresh() {
      showError('');
      try {
        const [ready, config, metrics] = await Promise.all([
          getJson('/readyz').catch(async () => getJson('/healthz')),
          getJson('/debug/config'),
          fetch('/metrics', { cache: 'no-store' }).then((r) => r.text())
        ]);
        state.ready = ready;
        state.config = config;
        state.metrics = metrics;
        state.loadedAt = new Date();
        render();
      } catch (err) {
        showError(err.message);
      }
    }

    function metric(name) {
      const line = state.metrics.split('\n').find((row) => row.startsWith(name + ' '));
      return line ? line.split(/\s+/).pop() : '0';
    }

    function routeMetrics() {
      return state.metrics.split('\n').filter((row) => row.startsWith('dxgate_agent_route_requests_total')).map((row) => {
        const labels = Object.fromEntries([...row.matchAll(/(\w+)="([^"]*)"/g)].map((m) => [m[1], m[2]]));
        const value = row.trim().split(/\s+/).pop();
        return { ...labels, requests: value };
      });
    }

    function table(id, columns, rows) {
      const head = '<tr>' + columns.map((c) => '<th>' + c.label + '</th>').join('') + '</tr>';
      const body = rows.length ? rows.map((row) => '<tr>' + columns.map((c) => '<td>' + c.value(row) + '</td>').join('') + '</tr>').join('') : '<tr><td colspan="' + columns.length + '" class="muted">No records from runtime endpoints</td></tr>';
      $(id).innerHTML = head + body;
    }

    function esc(value) {
      return String(value ?? '').replace(/[&<>"']/g, (ch) => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[ch]));
    }

    function tags(values, cls = '') {
      return (values || []).map((v) => '<span class="tag ' + cls + '">' + esc(v) + '</span>').join('');
    }

    function backendType(backend) {
      if (backend.type) return backend.type;
      if (backend.provider) return 'llm';
      if (backend.tools) return 'mcp';
      if (backend.agent) return 'a2a';
      return 'http';
    }

    function cfgList(name) {
      return Array.isArray(state.config?.[name]) ? state.config[name] : [];
    }

    function dubboRoutes() {
      return cfgList('listeners').flatMap((listener) =>
        (listener.virtual_hosts || []).flatMap((host) =>
          (host.routes || []).map((route) => ({
            listener: listener.name,
            virtualHost: host.name,
            route: route.name,
            domains: host.domains || [],
            weightedClusters: route.weighted_clusters || []
          }))
        )
      );
    }

    function render() {
      const ready = Boolean(state.ready && state.ready.ready);
      $('metric-ready').textContent = ready ? 'yes' : 'no';
      $('metric-routes').textContent = cfgList('routes').length;
      $('metric-backends').textContent = cfgList('backends').length;
      $('metric-denies').textContent = metric('dxgate_policy_denied_total');
      $('config-json').textContent = JSON.stringify(state.config, null, 2);
      syncPlaygroundFromRuntime();
      $('source-line').textContent = 'source: /debug/config + /readyz + /metrics' + (state.loadedAt ? ' · loaded ' + state.loadedAt.toLocaleTimeString() : '');
      $('statusline').innerHTML = [
        '<span class="pill ' + (ready ? 'ok' : 'bad') + '">' + (ready ? 'ready' : 'not ready') + '</span>',
        '<span class="pill">version ' + esc(state.ready?.version || state.config?.version || '-') + '</span>',
        '<span class="pill">' + esc((state.ready?.conflicts || []).length) + ' conflicts</span>'
      ].join('');
      table('traffic-table', [
        { label: 'Protocol', value: (r) => tags([r.protocol], r.protocol) },
        { label: 'Route', value: (r) => esc(r.route) },
        { label: 'Backend', value: (r) => esc(r.backend) },
        { label: 'Requests', value: (r) => esc(r.requests) }
      ], routeMetrics());
      table('routes-table', [
        { label: 'Protocol', value: (r) => tags([r.protocol], r.protocol) },
        { label: 'Name', value: (r) => esc(r.name) },
        { label: 'Matches', value: (r) => '<code>' + esc(JSON.stringify(r.matches || [])) + '</code>' },
        { label: 'Backends', value: (r) => tags((r.weighted_backends || []).map((b) => b.name + ':' + b.weight)) },
        { label: 'Policies', value: (r) => tags(r.policies) }
      ], cfgList('routes'));
      table('dubbo-routes-table', [
        { label: 'Listener', value: (r) => esc(r.listener) },
        { label: 'Virtual host', value: (r) => esc(r.virtualHost) },
        { label: 'Route', value: (r) => esc(r.route) },
        { label: 'Domains', value: (r) => tags(r.domains) },
        { label: 'Clusters', value: (r) => tags((r.weightedClusters || []).map((c) => c.name + ':' + c.weight)) }
      ], dubboRoutes());
      table('backends-table', [
        { label: 'Type', value: (r) => tags([backendType(r)], backendType(r)) },
        { label: 'Name', value: (r) => esc(r.name) },
        { label: 'Endpoint/provider', value: (r) => esc(r.endpoint || r.provider || '-') },
        { label: 'Models/tools/agent', value: (r) => tags(r.models || r.tools || (r.agent ? [r.agent] : [])) },
        { label: 'Policies', value: (r) => tags(r.policies) }
      ], cfgList('backends'));
      table('providers-table', [
        { label: 'Name', value: (r) => esc(r.name) },
        { label: 'Kind', value: (r) => esc(r.kind) },
        { label: 'Base URL', value: (r) => esc(r.base_url) },
        { label: 'Key env', value: (r) => esc(r.api_key_env || '-') }
      ], cfgList('providers'));
      table('policies-table', [
        { label: 'Name', value: (r) => esc(r.name) },
        { label: 'Action', value: (r) => esc(r.action || 'allow') },
        { label: 'Auth', value: (r) => esc(r.auth?.type || '-') },
        { label: 'Rate', value: (r) => r.rate_limit ? esc(r.rate_limit.requests + '/' + r.rate_limit.window_seconds + 's') : '-' },
        { label: 'Retry', value: (r) => r.retry ? esc(r.retry.attempts + ' attempts') : '-' }
      ], cfgList('policies'));
    }

    function syncPlaygroundFromRuntime() {
      if ($('mcp-path').dataset.touched) return;
      const route = cfgList('routes').find((item) => item.protocol === 'mcp');
      const path = route?.matches?.[0]?.path?.value;
      if (path) $('mcp-path').value = path;
    }

    function setTab(tab) {
      const button = document.querySelector('.nav button[data-tab="' + tab + '"]') || document.querySelector('.nav button[data-tab="overview"]');
      document.querySelectorAll('.nav button').forEach((b) => b.classList.toggle('active', b === button));
      $('title').textContent = button.dataset.title;
      document.querySelectorAll('main section').forEach((section) => section.classList.add('hidden'));
      $('tab-' + button.dataset.tab).classList.remove('hidden');
      history.replaceState(null, '', '#' + button.dataset.tab);
    }

    document.querySelectorAll('.nav button').forEach((button) => {
      button.addEventListener('click', () => setTab(button.dataset.tab));
    });
    $('refresh').addEventListener('click', refresh);
    $('copy-config').addEventListener('click', () => navigator.clipboard?.writeText(JSON.stringify(state.config, null, 2)));
    $('mcp-base').value = proxyBase();
    $('mcp-path').addEventListener('input', () => $('mcp-path').dataset.touched = 'true');
    $('mcp-send').addEventListener('click', async () => {
      showError('');
      $('mcp-result').textContent = '';
      try {
        const response = await fetch($('mcp-base').value.replace(/\/$/, '') + $('mcp-path').value, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: $('mcp-body').value
        });
        const text = await response.text();
        try { $('mcp-result').textContent = JSON.stringify(JSON.parse(text), null, 2); }
        catch (_) { $('mcp-result').textContent = text; }
      } catch (err) {
        $('mcp-result').textContent = err.message;
      }
    });
    setTab(location.hash ? location.hash.slice(1) : 'overview');
    refresh();
  </script>
</body>
</html>
"#;

#[cfg(test)]
mod tests {
    use super::admin_html;

    #[test]
    fn admin_ui_contains_runtime_panels() {
        let html = admin_html(18080);

        assert!(html.contains("Overview"));
        assert!(html.contains("/debug/config"));
        assert!(html.contains("/metrics"));
        assert!(html.contains("const proxyPort = 18080;"));
        assert!(html.contains("MCP request"));
        assert!(html.contains("/assets/dxgate-logo.svg"));
        assert!(!html.contains("class=\"mark\""));
        assert!(!html.contains("<strong>dxgate</strong>"));
        assert!(!html.contains("<span>admin</span>"));
        assert!(!html.contains("getJson('/debug/backends')"));
        assert!(!html.contains("getJson('/debug/policies')"));
        assert!(!html.contains("getJson('/debug/routes')"));
        assert!(!html.contains("metric-routes\">0"));
        assert!(!html.contains("value=\"/mcp\""));
        assert!(!html.contains("mcp-result\">{}"));
    }
}
