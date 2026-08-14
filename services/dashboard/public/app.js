/* ═══════════════════════════════════════════════════════════
   Weather Telemetry Dashboard — Client Logic
   ═══════════════════════════════════════════════════════════ */

// ─── State ──────────────────────────────────────────────────

let autoRefresh    = true;
let refreshTimer   = null;
let weatherData    = [];
let sortCol        = "city_name";
let sortAsc        = true;
let selectedCity   = null;
let selectedRange  = "1h";
let tempChart      = null;
let pipelineChart  = null;
let pipelineHistoryLoaded = false;

const POLL_INTERVAL = 5000;
const TOTAL_LOCATIONS = 500; // matches locations.ts
const BACKPRESSURE_THRESHOLD = 5000;

// ─── Init ───────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  setupToggle();
  setupSearch();
  setupSort();
  setupRangeButtons();
  setupTooltips();
  initPipelineChart();
  fetchAll();
  startPolling();
});

// ─── Auto-Refresh Toggle ────────────────────────────────────

function setupToggle() {
  const btn = document.getElementById("refresh-toggle");
  btn.classList.add("active");
  btn.addEventListener("click", () => {
    autoRefresh = !autoRefresh;
    btn.classList.toggle("active", autoRefresh);
    if (autoRefresh) startPolling();
    else stopPolling();
  });
}

function startPolling() {
  stopPolling();
  refreshTimer = setInterval(fetchAll, POLL_INTERVAL);
}

function stopPolling() {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
}

// ─── Fetch All Data ─────────────────────────────────────────

async function fetchAll() {
  await Promise.allSettled([
    fetchPipelineStatus(),
    fetchPipelineHistory(),
    fetchSummary(),
    fetchLatestWeather(),
    fetchStreamFeed(),
  ]);
  document.getElementById("last-updated").textContent =
    "Updated " + new Date().toLocaleTimeString();
}

// ─── Pipeline Status ─────────────────────────────────────────

async function fetchPipelineStatus() {
  try {
    const res  = await fetch("/api/pipeline/status");
    const data = await res.json();

    // ── Pipeline Health (header) ──

    const healthDot   = document.getElementById("pipeline-health");
    const healthLabel = document.getElementById("pipeline-health-label");
    if (healthDot && healthLabel) {
      healthDot.className   = "health-dot " + data.health;
      healthLabel.textContent = data.health === "healthy" ? "Healthy" : "Degraded";
    }

    // InfluxDB status chip
    const influxChip = document.getElementById("influx-status");
    if (influxChip) {
      const ok = data.services?.influxdb === "ok";
      influxChip.textContent = ok ? "InfluxDB ✓" : "InfluxDB ✗";
      influxChip.className   = "infra-chip " + (ok ? "ok" : "error");
    }

    // ── Fetcher group ──

    // Cycle chip: "#12  t+45s"
    const cycleId    = data.cycle.id ?? "—";
    const elapsedSec = data.cycle.elapsed;
    setText("val-cycle", `#${cycleId}`);
    setText("val-cycle-elapsed", elapsedSec != null ? `t+${elapsedSec}s` : "");
    setChipState("chip-cycle", "ok");

    // Queue chip: depth / 500 with mini progress bar
    const qDepth = data.queue.depth;
    const qPct   = Math.min(100, Math.round((qDepth / TOTAL_LOCATIONS) * 100));
    setText("val-queue", `${qDepth} / ${TOTAL_LOCATIONS}`);
    setText("val-queue-sub", `${qPct}% remaining`);
    const bar = document.getElementById("queue-progress-bar");
    if (bar) bar.style.width = qPct + "%";
    setChipState("chip-queue", "ok");

    // Rate limiter chip: "8 req/s | OK" or "COOLDOWN 24s"
    const rps = data.rateLimiter.rps ?? 8;
    const cooldownCount = data.rateLimiter.cooldownCount ?? 0;
    if (data.rateLimiter.cooldownActive) {
      const secs = Math.ceil(data.rateLimiter.cooldownTtlMs / 1000);
      setText("val-cooldown", `COOLDOWN ${secs}s`);
      setText("val-cooldown-sub", `${cooldownCount} cooldown${cooldownCount !== 1 ? "s" : ""} total`);
      setChipState("chip-cooldown", "warn");
    } else {
      setText("val-cooldown", `${rps} req/s`);
      setText("val-cooldown-sub", cooldownCount > 0 ? `${cooldownCount} cooldown${cooldownCount !== 1 ? "s" : ""} total` : "OK");
      setChipState("chip-cooldown", "ok");
    }

    // Fetch Latency chip (from Prometheus histogram avg)
    const fetchAvgMs = data.fetchMetrics?.fetchAvgMs;
    if (fetchAvgMs != null) {
      setText("val-fetch-latency", fetchAvgMs);
      setChipState("chip-fetch-latency", fetchAvgMs > 2000 ? "warn" : "ok");
    } else {
      setText("val-fetch-latency", "—");
      setChipState("chip-fetch-latency", "ok");
    }

    // ── Processor group ──

    // Stream chip: shows XLEN / ~MAXLEN for context
    const streamLen = data.stream.length;
    const maxLen    = data.stream.maxLen ?? 10000;
    setText("val-stream", `${streamLen.toLocaleString()} / ~${maxLen.toLocaleString()}`);
    setText("val-stream-sub", "XLEN / MAXLEN");
    setChipState("chip-stream", "ok");

    // Pending chip: un-ACKed messages — the real backpressure signal.
    const pending = data.stream.pending;
    const bp      = data.backpressure;
    setText("val-pending", pending);
    if (bp.state === "critical") {
      setChipState("chip-pending", "crit");
    } else if (bp.state === "warning") {
      setChipState("chip-pending", "warn");
    } else {
      setChipState("chip-pending", "ok");
    }

    // E2E Latency chip
    const e2eAvgMs = data.processor?.e2eAvgMs;
    const e2eStale = data.processor?.e2eStale;
    if (e2eStale) {
      // Processor is draining old backlog — latency not meaningful right now
      setText("val-e2e-latency", "Catch-up");
      setChipState("chip-e2e-latency", "warn");
    } else if (e2eAvgMs != null) {
      // Format: <1s → "842ms", <60s → "4.2s", ≥60s → "3m 30s"
      let e2eLabel;
      if (e2eAvgMs < 1000) {
        e2eLabel = `${e2eAvgMs}ms`;
      } else if (e2eAvgMs < 60000) {
        e2eLabel = `${(e2eAvgMs / 1000).toFixed(1)}s`;
      } else {
        const mins = Math.floor(e2eAvgMs / 60000);
        const secs = Math.round((e2eAvgMs % 60000) / 1000);
        e2eLabel = `${mins}m ${secs}s`;
      }
      setText("val-e2e-latency", e2eLabel);
      setChipState("chip-e2e-latency", e2eAvgMs > 10000 ? "warn" : "ok");
    } else {
      setText("val-e2e-latency", "—");
      setChipState("chip-e2e-latency", "ok");
    }

    // Success Rate chip
    const successRate = data.fetchMetrics?.successRate;
    const failedTotal = data.fetchMetrics?.failedTotal ?? 0;
    if (successRate != null) {
      setText("val-success-rate", `${successRate}%`);
      setText("val-success-sub", `${failedTotal} failed`);
      setChipState("chip-success-rate", successRate < 90 ? "warn" : "ok");
    } else {
      setText("val-success-rate", "—");
      setText("val-success-sub", "");
      setChipState("chip-success-rate", "ok");
    }

  } catch (err) {
    console.error("Pipeline status error:", err);
  }
}

// ─── Pipeline History Chart ──────────────────────────────────

function initPipelineChart() {
  const ctx = document.getElementById("pipeline-history-chart");
  if (!ctx) return;

  pipelineChart = new Chart(ctx.getContext("2d"), {
    type: "line",
    data: {
      labels: [],
      datasets: [
        {
          label: "Queue depth",
          data: [],
          borderColor: "#3b82f6",
          backgroundColor: "rgba(59,130,246,0.06)",
          borderWidth: 1.5,
          pointRadius: 0,
          tension: 0.2,
          fill: true,
          yAxisID: "yQueue",
        },
        {
          label: "Stream depth",
          data: [],
          borderColor: "#22d3ee",
          backgroundColor: "rgba(34,211,238,0.04)",
          borderWidth: 1.5,
          pointRadius: 0,
          tension: 0.2,
          fill: false,
          yAxisID: "yStream",
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { intersect: false, mode: "index" },
      plugins: {
        legend: {
          display: true,
          position: "top",
          align: "end",
          labels: {
            color: "#6b7280",
            font: { size: 10 },
            boxWidth: 12,
            padding: 8,
          },
        },
        tooltip: {
          backgroundColor: "#1a2332",
          borderColor: "#2a3a50",
          borderWidth: 1,
          titleColor: "#e5e7eb",
          bodyColor: "#9ca3af",
          padding: 8,
          displayColors: true,
        },
      },
      scales: {
        x: {
          ticks: {
            color: "#6b7280",
            font: { size: 10 },
            maxTicksLimit: 6,
            maxRotation: 0,
          },
          grid: { color: "rgba(42,58,80,0.3)" },
        },
        yQueue: {
          position: "left",
          min: 0,
          suggestedMax: TOTAL_LOCATIONS,
          ticks: {
            color: "#3b82f6",
            font: { size: 10 },
            maxTicksLimit: 4,
            callback: (v) => v === 0 ? "0" : v,
          },
          grid: { color: "rgba(42,58,80,0.3)" },
          title: { display: false },
        },
        yStream: {
          position: "right",
          min: 0,
          suggestedMax: 12000,
          ticks: {
            color: "#22d3ee",
            font: { size: 10 },
            maxTicksLimit: 4,
            callback: (v) => v >= 1000 ? (v / 1000).toFixed(0) + "k" : v,
          },
          grid: { drawOnChartArea: false },
          title: { display: false },
        },
      },
    },
  });
}

async function fetchPipelineHistory() {
  try {
    const res     = await fetch("/api/pipeline/history");
    const history = await res.json();
    if (!history || history.length === 0) return;

    const labels  = history.map(h => new Date(h.ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }));
    const queues  = history.map(h => h.queueDepth);
    const streams = history.map(h => h.streamLength);

    if (pipelineChart) {
      pipelineChart.data.labels = labels;
      pipelineChart.data.datasets[0].data = queues;
      pipelineChart.data.datasets[1].data = streams;
      pipelineChart.update("none"); // no animation on update
    }
  } catch (err) {
    console.error("Pipeline history error:", err);
  }
}

// ─── Summary Cards ──────────────────────────────────────────

async function fetchSummary() {
  try {
    const res  = await fetch("/api/weather/summary");
    const data = await res.json();

    setText("sum-cities", data.totalCities);

    if (data.hottestCity) {
      setText("sum-hottest-temp", `${data.hottestCity.temp}°C`);
      setText("sum-hottest-city", data.hottestCity.name);
    }
    if (data.coldestCity) {
      setText("sum-coldest-temp", `${data.coldestCity.temp}°C`);
      setText("sum-coldest-city", data.coldestCity.name);
    }

    setText("sum-condition", data.topCondition);
  } catch (err) {
    console.error("Summary error:", err);
  }
}

// ─── Weather Table ──────────────────────────────────────────

async function fetchLatestWeather() {
  try {
    const res = await fetch("/api/weather/latest");
    weatherData = await res.json();
    renderTable();
  } catch (err) {
    console.error("Weather error:", err);
  }
}

function renderTable() {
  const tbody  = document.getElementById("weather-tbody");
  const search = document.getElementById("city-search").value.toLowerCase();

  let filtered = weatherData;
  if (search) {
    filtered = weatherData.filter(r =>
      r.city_name.toLowerCase().includes(search)
    );
  }

  filtered.sort((a, b) => {
    let va = a[sortCol];
    let vb = b[sortCol];
    if (typeof va === "string") va = va.toLowerCase();
    if (typeof vb === "string") vb = vb.toLowerCase();
    if (va < vb) return sortAsc ? -1 : 1;
    if (va > vb) return sortAsc ? 1 : -1;
    return 0;
  });

  if (filtered.length === 0) {
    tbody.innerHTML = '<tr><td colspan="4" class="empty-state">No data available</td></tr>';
    return;
  }

  tbody.innerHTML = filtered.map(r => {
    const time = r.recorded_at
      ? new Date(r.recorded_at).toLocaleTimeString()
      : "—";
    const isSelected = r.city_name === selectedCity;
    return `<tr class="${isSelected ? "selected" : ""}" data-city="${r.city_name}">
      <td>${r.city_name}</td>
      <td>${r.temperature}°C</td>
      <td>${r.weather_condition}</td>
      <td>${time}</td>
    </tr>`;
  }).join("");

  tbody.querySelectorAll("tr[data-city]").forEach(row => {
    row.addEventListener("click", () => {
      selectedCity = row.dataset.city;
      renderTable();
      loadCityDetail(selectedCity);
    });
  });
}

function setupSearch() {
  document.getElementById("city-search").addEventListener("input", renderTable);
}

function setupSort() {
  document.querySelectorAll("#weather-table th[data-sort]").forEach(th => {
    th.addEventListener("click", () => {
      const col = th.dataset.sort;
      if (sortCol === col) sortAsc = !sortAsc;
      else { sortCol = col; sortAsc = true; }
      renderTable();
    });
  });
}

// ─── City Detail / Chart ────────────────────────────────────

function setupRangeButtons() {
  document.querySelectorAll(".range-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".range-btn").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      selectedRange = btn.dataset.range;
      if (selectedCity) loadCityDetail(selectedCity);
    });
  });
}

async function loadCityDetail(city) {
  const nameEl        = document.getElementById("detail-city-name");
  const hintEl        = document.getElementById("detail-hint");
  const chartContainer = document.getElementById("chart-container");

  nameEl.textContent = city;
  hintEl.style.display = "none";
  chartContainer.style.display = "block";

  try {
    const res  = await fetch(`/api/weather/${encodeURIComponent(city)}/history?range=${selectedRange}`);
    const data = await res.json();

    if (!data.points || data.points.length === 0) {
      nameEl.textContent = city + " (no history yet)";
      return;
    }

    renderChart(data.points, city);
  } catch (err) {
    console.error("City detail error:", err);
  }
}

function renderChart(points, city) {
  const ctx = document.getElementById("temp-chart").getContext("2d");

  if (tempChart) tempChart.destroy();

  const labels = points.map(p => {
    const d = new Date(p.recorded_at);
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  });

  const temps = points.map(p => p.temperature);

  tempChart = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [{
        label: `${city} — Temperature (°C)`,
        data: temps,
        borderColor: "#22d3ee",
        backgroundColor: "rgba(34, 211, 238, 0.08)",
        borderWidth: 2,
        pointRadius: temps.length > 50 ? 0 : 3,
        pointHoverRadius: 5,
        pointBackgroundColor: "#22d3ee",
        tension: 0.3,
        fill: true,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { intersect: false, mode: "index" },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "#1a2332",
          borderColor: "#2a3a50",
          borderWidth: 1,
          titleColor: "#e5e7eb",
          bodyColor: "#9ca3af",
          padding: 10,
          displayColors: false,
        }
      },
      scales: {
        x: {
          ticks: { color: "#6b7280", maxRotation: 0, maxTicksLimit: 8, font: { size: 11 } },
          grid: { color: "rgba(42, 58, 80, 0.4)" },
        },
        y: {
          ticks: { color: "#6b7280", font: { size: 11 }, callback: (v) => Math.round(v * 10) / 10 + "°" },
          grid: { color: "rgba(42, 58, 80, 0.4)" },
        }
      }
    }
  });
}

// ─── Live Stream Feed ───────────────────────────────────────

async function fetchStreamFeed() {
  try {
    const res     = await fetch("/api/stream/recent?count=20");
    const entries = await res.json();
    const feed    = document.getElementById("stream-feed");

    if (!entries || entries.length === 0) {
      feed.innerHTML = '<div class="empty-state">Waiting for stream entries...</div>';
      return;
    }

    feed.innerHTML = entries.map(e => {
      const time = e.recorded_at
        ? new Date(e.recorded_at).toLocaleTimeString()
        : "—";
      return `<div class="stream-entry">
        <span class="stream-city">${e.city_name || "—"}</span>
        <span class="stream-temp">${e.temperature || "—"}°C</span>
        <span class="stream-condition">${e.weather_condition || "—"}</span>
        <span class="stream-time">${time}</span>
      </div>`;
    }).join("");
  } catch (err) {
    console.error("Stream feed error:", err);
  }
}

// ─── Tooltips ───────────────────────────────────────────────

function setupTooltips() {
  const tooltip = document.getElementById("tooltip");
  if (!tooltip) return;

  document.querySelectorAll("[data-tooltip]").forEach(el => {
    el.addEventListener("mouseenter", (e) => {
      tooltip.textContent = el.dataset.tooltip;
      tooltip.classList.add("visible");
      positionTooltip(e);
    });
    el.addEventListener("mousemove", positionTooltip);
    el.addEventListener("mouseleave", () => {
      tooltip.classList.remove("visible");
    });
  });

  function positionTooltip(e) {
    const margin = 12;
    let x = e.clientX + margin;
    let y = e.clientY + margin;
    // Prevent overflow
    if (x + 280 > window.innerWidth) x = e.clientX - 280 - margin;
    if (y + 80  > window.innerHeight) y = e.clientY - 80  - margin;
    tooltip.style.left = x + "px";
    tooltip.style.top  = y + "px";
  }
}

// ─── Helpers ────────────────────────────────────────────────

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function setChipState(id, state) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.remove("ok", "warn", "crit");
  el.classList.add(state);
}
