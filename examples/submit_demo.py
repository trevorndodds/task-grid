from __future__ import annotations

from taskgrid.sdk import TaskGridClient

client = TaskGridClient("http://127.0.0.1:8000")
job = client.submit(
    name="demo square job",
    task_type="square",
    tasks=[{"x": i} for i in range(20)],
    priority=10,
    max_retries=2,
)
print("submitted", job["id"])
final = client.wait(job["id"], poll_seconds=0.5, timeout_seconds=60)
print("final", final)
print(client.results(job["id"]))
