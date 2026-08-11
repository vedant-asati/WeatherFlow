/* ═══════════════════════════════════════════════════════════
   Weather Telemetry Dashboard — Client Logic
   ═══════════════════════════════════════════════════════════ */

// ─── State ──────────────────────────────────────────────────

let autoRefresh = true;
let refreshTimer = null;
let weatherData = [];
let sortCol = "city_name";
let sortAsc = true;
let selectedCity = null;
let selectedRange = "1h";
let tempChart = null;

const POLL_INTERVAL = 5000;

// ─── Init ───────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  setupToggle();
  setupSearch();
  setupSort();
  setupRangeButtons();
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
    fetchSummary(),
    fetchLatestWeather(),
    fetchStreamFeed(),
  ]);
  document.getElementById("last-updated").textContent =
    "Updated " + new Date().toLocaleTimeString();
}

// ─── Pipeline Status ────────────────────────────────────────

async function fetchPipelineStatus() {
  try {
    const res = await fetch("/api/pipeline/status");
    const data = await res.json();

    setText("val-cycle", `#${data.cycle.id}`);
    setText("val-queue", data.queue.depth);
    setText("val-stream", data.stream.length);
    setText("val-pending", data.stream.pending);

    // Backpressure
    const bp = data.backpressure;
    setText("val-backpressure", bp.state.toUpperCase());
    setChipState("chip-backpressure",
      bp.state === "ok" ? "ok" : bp.state === "warning" ? "warn" : "crit"
    );

    // Cooldown
    if (data.rateLimiter.cooldownActive) {
      setText("val-cooldown", `${Math.ceil(data.rateLimiter.cooldownTtlMs / 1000)}s`);
      setChipState("chip-cooldown", "warn");
    } else {
      setText("val-cooldown", "OK");
      setChipState("chip-cooldown", "ok");
    }

    // Color-code stream chip based on depth
    const streamLen = data.stream.length;
    const threshold = data.backpressure.threshold;
    if (streamLen > threshold) setChipState("chip-stream", "crit");
    else if (streamLen > threshold * 0.7) setChipState("chip-stream", "warn");
    else setChipState("chip-stream", "ok");

    // Pending chip
    if (data.stream.pending > 100) setChipState("chip-pending", "warn");
    else setChipState("chip-pending", "ok");

    // Queue chip — 0 means cycle is complete
    setChipState("chip-queue", data.queue.depth > 0 ? "ok" : "ok");

  } catch (err) {
    console.error("Pipeline status error:", err);
  }
}

// ─── Summary Cards ──────────────────────────────────────────

async function fetchSummary() {
  try {
    const res = await fetch("/api/weather/summary");
    const data = await res.json();

    setText("sum-cities", data.totalCities);
    setText("sum-avg-temp", data.avgTemperature + "°");
    setText("sum-min-temp", data.minTemperature + "°");
    setText("sum-max-temp", data.maxTemperature + "°");
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
  const tbody = document.getElementById("weather-tbody");
  const search = document.getElementById("city-search").value.toLowerCase();

  let filtered = weatherData;
  if (search) {
    filtered = weatherData.filter(r =>
      r.city_name.toLowerCase().includes(search)
    );
  }

  // Sort
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

  // Row click handlers
  tbody.querySelectorAll("tr[data-city]").forEach(row => {
    row.addEventListener("click", () => {
      selectedCity = row.dataset.city;
      renderTable(); // re-render to update selected class
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
  const nameEl = document.getElementById("detail-city-name");
  const hintEl = document.getElementById("detail-hint");
  const chartContainer = document.getElementById("chart-container");

  nameEl.textContent = city;
  hintEl.style.display = "none";
  chartContainer.style.display = "block";

  try {
    const res = await fetch(`/api/weather/${encodeURIComponent(city)}/history?range=${selectedRange}`);
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
      interaction: {
        intersect: false,
        mode: "index",
      },
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
          ticks: {
            color: "#6b7280",
            maxRotation: 0,
            maxTicksLimit: 8,
            font: { size: 11 },
          },
          grid: { color: "rgba(42, 58, 80, 0.4)" },
        },
        y: {
          ticks: {
            color: "#6b7280",
            font: { size: 11 },
            callback: (v) => v + "°",
          },
          grid: { color: "rgba(42, 58, 80, 0.4)" },
        }
      }
    }
  });
}

// ─── Live Stream Feed ───────────────────────────────────────

async function fetchStreamFeed() {
  try {
    const res = await fetch("/api/stream/recent?count=20");
    const entries = await res.json();

    const feed = document.getElementById("stream-feed");

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
