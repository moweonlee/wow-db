// WOW-DB Web Monitoring Dashboard
// GET /dashboard — HTML 모니터링 대시보드 메인 페이지
// 3개 탭: Cluster / Storage / LSM Status
// JS 자동갱신 5초, 외부 CSS/JS 의존성 없음

use axum::{extract::State, response::Html};

use super::server::WebUiState;

// ─── WOW-DB ASCII 로고 ────────────────────────────────────────────────────────

const WOW_DB_LOGO: &str = r#"
██╗    ██╗ ██████╗ ██╗    ██╗      ██████╗ ██████╗
██║    ██║██╔═══██╗██║    ██║      ██╔══██╗██╔══██╗
██║ █╗ ██║██║   ██║██║ █╗ ██║█████╗██║  ██║██████╔╝
██║███╗██║██║   ██║██║███╗██║╚════╝██║  ██║██╔══██╗
╚███╔███╔╝╚██████╔╝╚███╔███╔╝      ██████╔╝██████╔╝
 ╚══╝╚══╝  ╚═════╝  ╚══╝╚══╝       ╚═════╝ ╚═════╝
"#;

// ─── 핸들러 ──────────────────────────────────────────────────────────────────

/// GET /dashboard?tab=cluster|storage|lsm — HTML 모니터링 대시보드
/// ?tab 파라미터로 초기 탭 지정 가능 → F5 새로고침 시 같은 탭 유지
pub async fn dashboard_handler(
    State(_state): State<WebUiState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Html<String> {
    let active_tab = params.get("tab")
        .map(|s| s.as_str())
        .unwrap_or("cluster");
    let active_tab = match active_tab {
        "storage" | "lsm" => active_tab,
        _ => "cluster",
    };
    Html(render_dashboard(active_tab))
}

/// GET / — /dashboard 로 리다이렉트
pub async fn root_redirect() -> axum::response::Redirect {
    axum::response::Redirect::permanent("/dashboard")
}

// ─── HTML 생성 ────────────────────────────────────────────────────────────────

fn render_dashboard(active_tab: &str) -> String {
    let cluster_active = if active_tab == "cluster" { " active" } else { "" };
    let storage_active = if active_tab == "storage" { " active" } else { "" };
    let lsm_active     = if active_tab == "lsm"     { " active" } else { "" };
    format!(
        r#"<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>WOW-DB Dashboard</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: monospace; background: #1a1a1a; color: #e0e0e0; padding: 10px; }}
    pre.logo {{ color: #4af; font-size: 11px; line-height: 1.2; margin-bottom: 6px; }}
    .subtitle {{ color: #888; font-size: 12px; margin-bottom: 14px; }}
    .tabs {{ display: flex; gap: 4px; margin-bottom: 12px; }}
    .tab-btn {{
      padding: 6px 18px; border: 1px solid #444; background: #2a2a2a;
      color: #aaa; cursor: pointer; font-family: monospace; font-size: 13px;
    }}
    .tab-btn.active {{ background: #3a3a3a; color: #fff; border-color: #4af; }}
    .tab-content {{ display: none; }}
    .tab-content.active {{ display: block; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 12px; font-size: 12px; }}
    th {{ background: #2a2a2a; color: #aaa; padding: 5px 8px; text-align: left; border: 1px solid #333; }}
    td {{ padding: 4px 8px; border: 1px solid #2a2a2a; }}
    tr:hover td {{ background: #222; }}
    .alive-yes {{ color: #4a4; }}
    .alive-no  {{ color: #f44; font-weight: bold; }}
    .warn {{ color: #fa0; }}
    .stop {{ color: #f44; font-weight: bold; }}
    .updated {{ color: #666; font-size: 11px; margin-top: 8px; }}
    .section-title {{ color: #4af; margin: 10px 0 6px; font-size: 13px; }}
    .error-msg {{ color: #f66; padding: 8px; background: #2a1a1a; margin: 8px 0; }}
    .empty-msg {{ color: #666; padding: 8px; }}
  </style>
</head>
<body>

<pre class="logo">{logo}</pre>
<div class="subtitle">Distributed OLAP Database — Monitoring Dashboard</div>

<div class="tabs">
  <button class="tab-btn{cluster_active}" onclick="showTab('cluster')">Cluster</button>
  <button class="tab-btn{storage_active}" onclick="showTab('storage')">Storage</button>
  <button class="tab-btn{lsm_active}"     onclick="showTab('lsm')">LSM Status</button>
</div>

<div id="tab-cluster" class="tab-content{cluster_active}">
  <div class="section-title">Query Nodes</div>
  <div id="qn-table"><div class="empty-msg">Loading...</div></div>
  <div class="section-title">Compute Nodes</div>
  <div id="cn-table"><div class="empty-msg">Loading...</div></div>
  <div class="section-title">Storage Nodes</div>
  <div id="dn-table"><div class="empty-msg">Loading...</div></div>
  <div class="updated" id="cluster-updated"></div>
</div>

<div id="tab-storage" class="tab-content{storage_active}">
  <div class="section-title">Tables (Cubes)</div>
  <div id="storage-table"><div class="empty-msg">Loading...</div></div>
  <div class="updated" id="storage-updated"></div>
</div>

<div id="tab-lsm" class="tab-content{lsm_active}">
  <div class="section-title">LSM Compaction Status (per Table per Level)</div>
  <div id="lsm-table"><div class="empty-msg">Loading...</div></div>
  <div class="updated" id="lsm-updated"></div>
</div>

<script>
// ─── Tab navigation — URL 에 ?tab=NAME 을 기록해 F5 새로고침 시 유지
function showTab(name) {{
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  event.target.classList.add('active');
  // URL 주소창 업데이트 (페이지 reload 없이)
  history.replaceState(null, '', '/dashboard?tab=' + name);
}}

// ─── Helpers
function ts() {{ return new Date().toLocaleTimeString(); }}
function esc(s) {{ return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }}

// ─── Cluster tab
async function fetchCluster() {{
  try {{
    const d = await fetch('/api/v1/cluster').then(r => r.json());
    const renderNodes = (nodes) => {{
      if (!nodes || nodes.length === 0) return '<div class="empty-msg">No nodes</div>';
      return '<table><tr><th>ID</th><th>Address</th><th>Role</th><th>Status</th></tr>'
        + nodes.map(n => `<tr>
            <td>${{esc(n.id)}}</td>
            <td>${{esc(n.address)}}</td>
            <td>${{esc(n.role)}}</td>
            <td class="${{n.alive ? 'alive-yes' : 'alive-no'}}">${{n.alive ? 'ONLINE' : 'OFFLINE'}}</td>
          </tr>`).join('')
        + '</table>';
    }};
    document.getElementById('qn-table').innerHTML = renderNodes(d.query_nodes);
    document.getElementById('cn-table').innerHTML = renderNodes(d.compute_nodes);
    document.getElementById('dn-table').innerHTML = renderNodes(d.data_nodes);
    document.getElementById('cluster-updated').textContent = 'Last updated: ' + ts();
  }} catch(e) {{
    document.getElementById('qn-table').innerHTML = '<div class="error-msg">Error: ' + esc(e.message) + '</div>';
  }}
}}

// ─── Storage tab
async function fetchStorage() {{
  try {{
    const resp = await fetch('/api/cubes').then(r => r.json());
    // /api/cubes 는 cubes 배열을 포함한 객체 또는 직접 배열 형태
    const cubes = Array.isArray(resp) ? resp : (resp.cubes || []);
    if (cubes.length === 0) {{
      document.getElementById('storage-table').innerHTML = '<div class="empty-msg">No tables found</div>';
      return;
    }}
    function fmtBytes(n) {{
      if (!n) return '-';
      if (n >= 1e9) return (n/1e9).toFixed(1) + ' GB';
      if (n >= 1e6) return (n/1e6).toFixed(1) + ' MB';
      if (n >= 1e3) return (n/1e3).toFixed(1) + ' KB';
      return n + ' B';
    }}
    let html = '<table><tr><th>Table</th><th>Database</th><th>Columns</th><th>Partitions</th><th>Rows</th><th>Size</th><th>Storage</th></tr>';
    cubes.forEach(c => {{
      const cols = c.column_count !== undefined ? c.column_count
                 : (c.columns || []).length;
      const colNames = (c.columns || []).map(col => esc(col.name || col)).join(', ');
      const rows = c.row_count > 0 ? c.row_count.toLocaleString() : '-';
      const size = fmtBytes(c.size_bytes);
      const parts = c.partition_count > 0 ? c.partition_count : '-';
      html += `<tr>
        <td title="${{esc(colNames)}}">${{esc(c.name)}}</td>
        <td>${{esc(c.database || 'default')}}</td>
        <td>${{esc(cols)}}</td>
        <td>${{parts}}</td>
        <td>${{rows}}</td>
        <td>${{size}}</td>
        <td>${{esc(c.storage_backend || c.storage || '-')}}</td>
      </tr>`;
    }});
    html += '</table>';
    document.getElementById('storage-table').innerHTML = html;
    document.getElementById('storage-updated').textContent = 'Last updated: ' + ts();
  }} catch(e) {{
    document.getElementById('storage-table').innerHTML = '<div class="error-msg">Error: ' + esc(e.message) + '</div>';
  }}
}}

// ─── LSM tab
async function fetchLsm() {{
  try {{
    const d = await fetch('/api/v1/lsm').then(r => r.json());
    const nodes = d.nodes || [];
    if (nodes.length === 0) {{
      document.getElementById('lsm-table').innerHTML = '<div class="empty-msg">No storage nodes found</div>';
      return;
    }}

    // Collect all level indices across all partitions
    let maxLevel = 0;
    nodes.forEach(n => (n.partitions || []).forEach(p => {{
      if (p.total_levels > maxLevel) maxLevel = p.total_levels;
    }}));

    let html = '<table><tr><th>SN</th><th>Table</th><th>Partition</th>';
    for (let l = 0; l <= maxLevel; l++) html += `<th>L${{l}} files</th>`;
    html += '<th>Compaction</th><th>Write</th></tr>';

    nodes.forEach(n => {{
      const parts = n.partitions || [];
      if (parts.length === 0) {{
        html += `<tr><td>${{esc(n.node_id)}}</td><td colspan="${{maxLevel + 4}}" style="color:#666">No partitions</td></tr>`;
        return;
      }}
      parts.forEach(p => {{
        const sizes = p.level_sizes || [];
        const wc = n.write_control || 'Unknown';
        const wcClass = wc === 'Stop' ? 'stop' : wc === 'Slowdown' ? 'warn' : '';
        html += `<tr>
          <td>${{esc(n.node_id)}}</td>
          <td>${{esc(p.cube_name)}}</td>
          <td>${{esc(p.partition_name)}}</td>`;
        for (let l = 0; l <= maxLevel; l++) {{
          const cnt = p.l0_file_count !== undefined && l === 0 ? p.l0_file_count
                    : (sizes[l] ? Math.ceil(sizes[l] / (64 * 1024 * 1024)) : 0);
          const style = l === 0 && cnt >= 4 ? ' class="warn"' : '';
          html += `<td${{style}}>${{cnt}}</td>`;
        }}
        html += `<td>${{esc(p.compaction_status || '-')}}</td>
          <td class="${{wcClass}}">${{esc(wc)}}</td>
        </tr>`;
      }});
    }});
    html += '</table>';
    document.getElementById('lsm-table').innerHTML = html;
    document.getElementById('lsm-updated').textContent = 'Last updated: ' + ts();
  }} catch(e) {{
    document.getElementById('lsm-table').innerHTML = '<div class="error-msg">Error: ' + esc(e.message) + '</div>';
  }}
}}

// ─── Initial load + auto-refresh
fetchCluster(); fetchStorage(); fetchLsm();
setInterval(fetchCluster, 5000);
setInterval(fetchStorage, 5000);
setInterval(fetchLsm, 5000);
</script>
</body>
</html>"#,
        logo = WOW_DB_LOGO
    )
}
