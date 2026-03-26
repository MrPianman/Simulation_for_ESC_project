const COLORS = {
  store: getCss('--store'),
  gas_station: getCss('--gas'),
  launch_point: getCss('--launch'),
  get_point: getCss('--get'),
};

const ALG_COLORS = ['#6ef2c1', '#7aa2ff', '#ffb454', '#f48fb1'];

const seedInput = document.getElementById('seed-input');
const runBtn = document.getElementById('run-btn');
const statusEl = document.getElementById('status');
const errorsEl = document.getElementById('errors');
const oldCanvas = document.getElementById('old-canvas');
const ourCanvas = document.getElementById('our-canvas');
const oldMeta = document.getElementById('old-meta');
const ourMeta = document.getElementById('our-meta');
const legendEl = document.getElementById('legend');
const runsInput = document.getElementById('runs-input');
const fullToggle = document.getElementById('fullrun-toggle');

function getCss(varName) {
  return getComputedStyle(document.documentElement).getPropertyValue(varName).trim();
}

function setStatus(text, cls = 'idle') {
  statusEl.textContent = text;
  statusEl.className = `status ${cls}`;
}

function setError(msg) {
  errorsEl.textContent = msg || '';
}

function buildLegend() {
  legendEl.innerHTML = '';
  const entries = [
    ['Store', COLORS.store],
    ['Gas station', COLORS.gas_station],
    ['Launch point', COLORS.launch_point],
    ['Get point', COLORS.get_point],
  ];
  entries.forEach(([label, color]) => {
    const div = document.createElement('div');
    div.className = 'swatch';
    const dot = document.createElement('span');
    dot.className = 'dot';
    dot.style.background = color;
    div.appendChild(dot);
    div.append(label);
    legendEl.appendChild(div);
  });
}

function normalizePayload(payload) {
  const points = payload.points || [];
  const pointMap = new Map(points.map((p) => [p.id, p]));
  const connections = {};
  const rawConn = payload.connections || {};
  Object.entries(rawConn).forEach(([k, v]) => {
    const nid = Number(k);
    connections[nid] = (v || []).map(Number);
  });
  return { pointMap, points, connections };
}

function getBounds(points) {
  if (!points.length) {
    return { minX: 0, maxX: 1, minY: 0, maxY: 1 };
  }
  const xs = points.map((p) => p.x);
  const ys = points.map((p) => p.y);
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const pad = Math.max((maxX - minX) * 0.05, 20);
  return { minX: minX - pad, maxX: maxX + pad, minY: minY - pad, maxY: maxY + pad };
}

function project(point, bounds, size) {
  const { minX, maxX, minY, maxY } = bounds;
  const scaleX = size.width / (maxX - minX || 1);
  const scaleY = size.height / (maxY - minY || 1);
  const x = (point.x - minX) * scaleX;
  const y = size.height - (point.y - minY) * scaleY;
  return { x, y };
}

function drawScene(canvas, payload, algoKey, bounds) {
  const ctx = canvas.getContext('2d');
  const { pointMap, points, connections } = normalizePayload(payload);
  const perCar = (payload.algorithms?.[algoKey]?.per_car) || [];

  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.lineWidth = 1;

  // connections
  ctx.strokeStyle = 'rgba(255,255,255,0.05)';
  Object.entries(connections).forEach(([k, neighbors]) => {
    const src = pointMap.get(Number(k));
    if (!src) return;
    neighbors.forEach((nid) => {
      const dst = pointMap.get(nid);
      if (!dst) return;
      const p1 = project(src, bounds, canvas);
      const p2 = project(dst, bounds, canvas);
      ctx.beginPath();
      ctx.moveTo(p1.x, p1.y);
      ctx.lineTo(p2.x, p2.y);
      ctx.stroke();
    });
  });

  // routes per car
  perCar.forEach((car, idx) => {
    const ids = car.quest_route_ids || [];
    if (ids.length < 2) return;
    ctx.strokeStyle = ALG_COLORS[idx % ALG_COLORS.length];
    ctx.lineWidth = 3;
    ctx.beginPath();
    ids.forEach((id, i) => {
      const pt = pointMap.get(id);
      if (!pt) return;
      const { x, y } = project(pt, bounds, canvas);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();
  });

  // points
  points.forEach((p) => {
    const { x, y } = project(p, bounds, canvas);
    ctx.beginPath();
    ctx.fillStyle = COLORS[p.type] || '#ccc';
    const r = p.type === 'launch_point' ? 6 : p.type === 'get_point' ? 5 : 4;
    ctx.arc(x, y, r, 0, Math.PI * 2);
    ctx.fill();
  });
}

async function runComparison(evt) {
  evt.preventDefault();
  setError('');
  setStatus('Running...', 'running');
  runBtn.disabled = true;

  const seedVal = seedInput.value.trim();
  const seed = seedVal === '' ? null : Number(seedVal);
  const fullrun = fullToggle.checked;
  const numRuns = Number(runsInput.value) || 100;

  let payload;
  try {
    if (fullrun) {
      payload = await runFullJob(numRuns);
    } else {
      const res = await fetch('/api/compare', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ seed: Number.isFinite(seed) ? seed : null }),
      });
      payload = await res.json();
      if (!res.ok || payload.error) {
        throw new Error(payload.error || `Request failed with status ${res.status}`);
      }
    }
  } catch (err) {
    setStatus('Error', 'error');
    setError(err.message);
    runBtn.disabled = false;
    return;
  }

  const comparePayload = fullrun ? payload.sample : payload;
  const bounds = getBounds(comparePayload.points || []);
  drawScene(oldCanvas, comparePayload, 'old', bounds);
  drawScene(ourCanvas, comparePayload, 'our', bounds);

  const algoData = fullrun ? payload.summaries : comparePayload.algorithms;
  const oldSummary = algoData?.old || {};
  const ourSummary = algoData?.our || {};
  oldMeta.textContent = summaryText(oldSummary);
  ourMeta.textContent = summaryText(ourSummary);

  setStatus('Done', 'idle');
  runBtn.disabled = false;
}

async function runFullJob(numRuns) {
  const startRes = await fetch('/api/fullrun', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ num_runs: numRuns, algo: 'both' }),
  });
  const startPayload = await startRes.json();
  if (!startRes.ok || startPayload.error) {
    throw new Error(startPayload.error || `Start failed with status ${startRes.status}`);
  }
  const jobId = startPayload.job_id;

  let attempt = 0;
  while (true) {
    await new Promise((r) => setTimeout(r, 800));
    attempt += 1;
    const statusRes = await fetch(`/api/fullrun/${jobId}`);
    const statusPayload = await statusRes.json();
    if (!statusRes.ok || statusPayload.error) {
      throw new Error(statusPayload.error || `Status failed with ${statusRes.status}`);
    }
    const pct = Math.round((statusPayload.progress || 0) * 100);
    setStatus(`Running full batch... ${pct}%`, 'running');
    if (statusPayload.status === 'done') {
      return statusPayload.result;
    }
    if (statusPayload.status === 'error') {
      throw new Error(statusPayload.error || 'Job failed');
    }
    if (attempt > 2000) {
      throw new Error('Job timeout');
    }
  }
}

function summaryText(summary) {
  const cause = summary.cause || 'n/a';
  const budget = summary.budget?.toFixed ? summary.budget.toFixed(2) : '0.00';
  const dist = summary.distance?.toFixed ? summary.distance.toFixed(2) : '0.00';
  return `Cause: ${cause} · Budget: ${budget} · Distance: ${dist}`;
}

buildLegend();
document.getElementById('control-form').addEventListener('submit', runComparison);
setStatus('Idle');
runComparison({ preventDefault: () => {} });
