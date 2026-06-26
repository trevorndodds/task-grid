from __future__ import annotations

from taskgrid.sdk import TaskGridClient

client = TaskGridClient("http://127.0.0.1:8000")
chunk_count = 24
samples_per_chunk = 25000
job = client.submit(
    name="toy monte carlo pi",
    task_type="monte_carlo_pi_chunk",
    tasks=[{"samples": samples_per_chunk, "offset": i * samples_per_chunk} for i in range(chunk_count)],
    priority=5,
)
print("submitted", job["id"])
client.wait(job["id"], poll_seconds=0.5, timeout_seconds=120)
results = client.results(job["id"])["results"]
inside = sum(r["result"]["value"]["inside"] for r in results if r["status"] == "succeeded")
samples = sum(r["result"]["value"]["samples"] for r in results if r["status"] == "succeeded")
print({"pi_estimate": 4 * inside / samples, "inside": inside, "samples": samples})
