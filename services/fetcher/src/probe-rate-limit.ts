/**
 * Rate limit behaviour probe — measures three things at each test rate:
 *
 *   1. TIME TO FIRST 429  — how long can we sustain this rate cleanly?
 *   2. STORM DURATION     — once 429s start, how long do they keep coming?
 *   3. RECOVERY TIME      — after we go completely silent, how many seconds
 *                           until Open-Meteo accepts requests again?
 *
 * Test rates: 25, 30, 35 req/s  (the interesting band around our limit)
 *
 * Between each test rate we wait for full recovery before starting the next,
 * so the rates don't bleed into each other.
 *
 * Run: npm run probe:rate
 */

import axios from "axios";
import * as https from "https";

const httpClient = axios.create({
  timeout: 10_000,
  httpsAgent: new https.Agent({ family: 4, maxSockets: 200, keepAlive: true }),
  validateStatus: () => true, // never throw on 4xx — we want to see 429s
});

const LOCATIONS = [
  { lat: 12.97,  lon: 77.59  }, // Bangalore
  { lat: 51.51,  lon: -0.13  }, // London
  { lat: 40.71,  lon: -74.01 }, // New York
  { lat: 35.69,  lon: 139.69 }, // Tokyo
  { lat: -33.87, lon: 151.21 }, // Sydney
  { lat: 48.85,  lon: 2.35   }, // Paris
  { lat: 55.75,  lon: 37.62  }, // Moscow
  { lat: 19.08,  lon: 72.88  }, // Mumbai
  { lat: -23.55, lon: -46.63 }, // São Paulo
  { lat: 30.04,  lon: 31.24  }, // Cairo
  { lat: 1.35,   lon: 103.82 }, // Singapore
  { lat: 41.01,  lon: 28.97  }, // Istanbul
  { lat: 37.57,  lon: 126.98 }, // Seoul
  { lat: -1.29,  lon: 36.82  }, // Nairobi
  { lat: 6.52,   lon: 3.38   }, // Lagos
];

const TEST_RATES    = [25, 30, 35];  // req/s to test
const MAX_RUN_SEC   = 120;           // max seconds to run at each rate before giving up
const RECOVERY_PROBE_INTERVAL = 2000; // ms between recovery probes (after stopping)
const MAX_RECOVERY_WAIT_SEC   = 120;  // give up waiting for recovery after this long

// ─── helpers ───────────────────────────────────────────────────────────────

function sleep(ms: number) {
  return new Promise<void>(r => setTimeout(r, ms));
}

async function fireOne(idx: number): Promise<{ ok: boolean; status: number; latencyMs: number }> {
  const loc = LOCATIONS[idx % LOCATIONS.length];
  const url = `https://api.open-meteo.com/v1/forecast?latitude=${loc.lat}&longitude=${loc.lon}&current_weather=true`;
  const t = Date.now();
  const { status } = await httpClient.get(url);
  return { ok: status === 200, status, latencyMs: Date.now() - t };
}

// ─── phase 1+2: sustain rate until 429s appear, measure storm duration ────

interface SustainResult {
  timeToFirst429Ms: number | null;  // null = never got a 429
  stormDurationMs:  number | null;  // how long 429s kept coming
  totalOk:    number;
  total429:   number;
  totalFired: number;
  secondStats: Array<{ sec: number; ok: number; denied: number }>;
}

async function sustainRate(rps: number): Promise<SustainResult> {
  const intervalMs   = 1000 / rps;
  const maxRequests  = rps * MAX_RUN_SEC;

  const secondStats: Array<{ sec: number; ok: number; denied: number }> = [];
  let totalOk    = 0;
  let total429   = 0;
  let totalFired = 0;

  let timeToFirst429Ms: number | null = null;
  let last429AtMs:      number | null = null;
  const startMs = Date.now();

  console.log(`\n  ── Phase 1+2: sustaining ${rps} req/s (max ${MAX_RUN_SEC}s) ──`);

  // Drip requests at exactly rps/s and collect results as they arrive
  await new Promise<void>(resolve => {
    let reqIdx = 0;
    let currentSec = 0;
    let secOk = 0;
    let secDenied = 0;

    const interval = setInterval(() => {
      if (reqIdx >= maxRequests) { clearInterval(interval); resolve(); return; }

      const myIdx = reqIdx++;
      const fireMs = Date.now();

      fireOne(myIdx).then(({ ok, status, latencyMs }) => {
        const nowMs  = Date.now();
        const sec    = Math.floor((fireMs - startMs) / 1000);

        // Roll over second bucket
        if (sec !== currentSec) {
          secondStats.push({ sec: currentSec, ok: secOk, denied: secDenied });
          process.stdout.write(
            `  t+${String(currentSec).padStart(3)}s` +
            `  ✓${String(secOk).padStart(3)}` +
            `  ✗${String(secDenied).padStart(3)}` +
            `  [${String(secOk + secDenied).padStart(3)} fired]\n`
          );
          currentSec = sec;
          secOk = 0;
          secDenied = 0;
        }

        if (ok) {
          totalOk++;
          secOk++;
        } else if (status === 429) {
          total429++;
          secDenied++;
          if (timeToFirst429Ms === null) timeToFirst429Ms = nowMs - startMs;
          last429AtMs = nowMs;
        }

        // Once we've seen 429s and they've stopped for 5s, we have enough data
        if (
          timeToFirst429Ms !== null &&
          last429AtMs !== null &&
          nowMs - last429AtMs > 5000
        ) {
          clearInterval(interval);
          resolve();
        }
      });
    }, intervalMs);
  });

  const stormDurationMs = (timeToFirst429Ms !== null && last429AtMs !== null)
    ? last429AtMs - (startMs + timeToFirst429Ms)
    : null;

  return { timeToFirst429Ms, stormDurationMs, totalOk, total429, totalFired: totalOk + total429, secondStats };
}

// ─── phase 3: measure recovery time ─────────────────────────────────────

async function measureRecovery(): Promise<number | null> {
  console.log(`\n  ── Phase 3: silence — probing recovery every ${RECOVERY_PROBE_INTERVAL / 1000}s ──`);
  const startMs = Date.now();
  let attempt = 0;

  while (Date.now() - startMs < MAX_RECOVERY_WAIT_SEC * 1000) {
    await sleep(RECOVERY_PROBE_INTERVAL);
    attempt++;
    const elapsed = ((Date.now() - startMs) / 1000).toFixed(1);

    // Fire 3 probes in parallel — if all succeed, we're recovered
    const probes = await Promise.all([fireOne(0), fireOne(1), fireOne(2)]);
    const allOk  = probes.every(p => p.ok);
    const any429 = probes.some(p => p.status === 429);

    console.log(
      `  silent +${elapsed}s — ` +
      probes.map(p => p.status === 200 ? "✓" : `✗(${p.status})`).join("  ")
    );

    if (allOk) {
      return Date.now() - startMs; // ms of silence needed to recover
    }
  }

  return null; // didn't recover within timeout
}

// ─── main ─────────────────────────────────────────────────────────────────

async function main() {
  console.log("━".repeat(60));
  console.log(" Open-Meteo rate limit behaviour probe");
  console.log(` Test rates: ${TEST_RATES.join(", ")} req/s`);
  console.log(" Measures: time-to-429 · storm duration · recovery time");
  console.log("━".repeat(60));

  const report: Array<{
    rps: number;
    timeToFirst429S: number | null;
    stormDurationS:  number | null;
    recoveryS:       number | null;
    totalOk:   number;
    total429:  number;
  }> = [];

  for (const rps of TEST_RATES) {
    console.log(`\n${"═".repeat(60)}`);
    console.log(` TESTING ${rps} req/s`);
    console.log("═".repeat(60));

    const { timeToFirst429Ms, stormDurationMs, totalOk, total429 } = await sustainRate(rps);

    let recoveryMs: number | null = null;
    if (total429 > 0) {
      recoveryMs = await measureRecovery();
    } else {
      console.log(`\n  No 429s at ${rps} req/s — skipping recovery probe.`);
    }

    const timeToFirst429S = timeToFirst429Ms !== null ? timeToFirst429Ms / 1000 : null;
    const stormDurationS  = stormDurationMs  !== null ? stormDurationMs  / 1000 : null;
    const recoveryS       = recoveryMs       !== null ? recoveryMs        / 1000 : null;

    report.push({ rps, timeToFirst429S, stormDurationS, recoveryS, totalOk, total429 });

    console.log(`\n  ── Summary for ${rps} req/s ──`);
    console.log(`  Total ok:           ${totalOk}`);
    console.log(`  Total 429s:         ${total429}`);
    console.log(`  Time to first 429:  ${timeToFirst429S !== null ? timeToFirst429S.toFixed(1) + "s" : "never"}`);
    console.log(`  Storm duration:     ${stormDurationS  !== null ? stormDurationS.toFixed(1)  + "s" : "n/a"}`);
    console.log(`  Recovery time:      ${recoveryS       !== null ? recoveryS.toFixed(1)       + "s" : "did not recover"}`);

    // Wait for full recovery between test rates so they don't bleed into each other
    if (total429 > 0 && recoveryMs === null) {
      console.log("\n  API did not recover — stopping probe early.");
      break;
    }

    if (rps !== TEST_RATES[TEST_RATES.length - 1]) {
      console.log(`\n  Waiting 10s extra buffer before next rate test...`);
      await sleep(10_000);
    }
  }

  // ─── final report ───────────────────────────────────────────────────────
  console.log(`\n\n${"━".repeat(60)}`);
  console.log(" FINAL REPORT");
  console.log("━".repeat(60));
  console.log(
    ` ${"Rate".padEnd(8)}` +
    ` ${"Time→429".padEnd(12)}` +
    ` ${"Storm".padEnd(10)}` +
    ` ${"Recovery".padEnd(12)}` +
    ` ${"Ok".padEnd(6)}` +
    ` 429s`
  );
  console.log("─".repeat(60));
  for (const r of report) {
    console.log(
      ` ${String(r.rps + " rps").padEnd(8)}` +
      ` ${r.timeToFirst429S !== null ? r.timeToFirst429S.toFixed(1) + "s" : "never     "}`.padEnd(12) +
      ` ${r.stormDurationS  !== null ? r.stormDurationS.toFixed(1)  + "s" : "n/a   "}`.padEnd(10) +
      ` ${r.recoveryS       !== null ? r.recoveryS.toFixed(1)       + "s" : "timeout   "}`.padEnd(12) +
      ` ${String(r.totalOk).padEnd(6)}` +
      ` ${r.total429}`
    );
  }
  console.log("━".repeat(60));
  console.log("\n Interpretation:");
  console.log("  Time→429  = how long your rate is safe per cycle");
  console.log("  Storm     = how long to wait in cooldown after a 429");
  console.log("  Recovery  = silence needed before requests work again");
  console.log("━".repeat(60));
}

main().catch(err => {
  console.error("Fatal:", err.message);
  process.exit(1);
});
