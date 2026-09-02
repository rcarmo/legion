const base = process.env.LEGION_LOAD_URL ?? "http://127.0.0.1:18080";
const requests = Number(process.env.LEGION_LOAD_REQUESTS ?? 1000);
const concurrency = Number(process.env.LEGION_LOAD_CONCURRENCY ?? 8);
const minRps = Number(process.env.LEGION_LOAD_MIN_RPS ?? 60);
const maxP95Ms = Number(process.env.LEGION_LOAD_MAX_P95_MS ?? 200);
const maxErrorRate = Number(process.env.LEGION_LOAD_MAX_ERROR_RATE ?? 0.001);
const apiKey = process.env.LEGION_LOAD_API_KEY ?? "";
const latencies: number[] = [];
let next = 0;
let errors = 0;

async function worker() {
  while (true) {
    const index = next++;
    if (index >= requests) return;
    const started = performance.now();
    try {
      const response = await fetch(`${base}/functions/load/invoke`, {
        method: "POST",
        headers: { "content-type": "application/json", ...(apiKey ? { authorization: `Bearer ${apiKey}` } : {}) },
        body: JSON.stringify({ index }),
      });
      if (!response.ok) errors++;
      await response.arrayBuffer();
    } catch {
      errors++;
    }
    latencies.push(performance.now() - started);
  }
}

const started = performance.now();
await Promise.all(Array.from({ length: concurrency }, worker));
const elapsedSeconds = (performance.now() - started) / 1000;
latencies.sort((a, b) => a - b);
const percentile = (value: number) => latencies[Math.min(latencies.length - 1, Math.ceil(latencies.length * value) - 1)] ?? 0;
const rps = requests / elapsedSeconds;
const errorRate = errors / requests;
const result = { requests, concurrency, rps, errorRate, p50Ms: percentile(0.5), p95Ms: percentile(0.95), p99Ms: percentile(0.99) };
console.log(JSON.stringify(result, null, 2));
if (rps < minRps || result.p95Ms > maxP95Ms || errorRate > maxErrorRate) process.exit(1);
