import json
import os
import shlex
import subprocess
import sys
import time
import psutil

BUILD_DIR = "build"
os.makedirs(BUILD_DIR, exist_ok=True)

TARGETS = {
    "moodycamel": {
        "src": "MoodyCamelQueue/producer_consumer_moodycamel.cpp",
        "cflags": "-IMoodyCamelQueue",
        "out": f"{BUILD_DIR}/moody_demo",
    },
    "queuebuffer": {
        "src": "QueueBuffer/producer_consumer_que.cpp",
        "cflags": "",
        "out": f"{BUILD_DIR}/queue_demo",
    },
    "ringbuffer": {
        "src": "RingBuffer/producer_consumer_rb.cpp",
        "cflags": "",
        "out": f"{BUILD_DIR}/ring_demo",
    },
    "semaphore": {
        "src": "SemaphoreBuffer/producer_consumer_sem.cpp",
        "cflags": "-std=c++20",
        "out": f"{BUILD_DIR}/sem_demo",
    },
    "mpmc": {
        "src": "MPMCQueue/producer_consumer_mpmc.cpp",
        "cflags": "",
        "out": f"{BUILD_DIR}/mpmc_demo",
    },
}


def build_target(name, info):
    cmd = f"g++ -std=c++17 -O2 -pthread {info['cflags']} {info['src']} -o {info['out']}"
    print("Building:", cmd)
    proc = subprocess.run(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        print(f"Build failed for {name}:", proc.stderr.decode())
        return False
    return True


def run_and_measure(cmd, timeout=None):
    start = time.time()
    p = subprocess.Popen(shlex.split(cmd))
    proc = psutil.Process(p.pid)
    peak_rss = 0
    start_cpu = sum(proc.cpu_times()[:2])
    try:
        while True:
            if p.poll() is not None:
                break
            try:
                mem = proc.memory_info().rss
                if mem > peak_rss:
                    peak_rss = mem
            except psutil.NoSuchProcess:
                break
            time.sleep(0.05)
    except Exception:
        p.kill()
        raise
    end = time.time()
    end_cpu = 0.0
    try:
        end_cpu = sum(proc.cpu_times()[:2])
    except Exception:
        end_cpu = 0.0
    return {
        "returncode": p.returncode,
        "runtime_s": end - start,
        "cpu_s": max(0.0, end_cpu - start_cpu),
        "peak_rss_bytes": peak_rss,
    }


def test_bench_all(tmp_path):
    # Unified parameters
    PRODUCER_COUNT = 4
    CONSUMER_COUNT = 4
    ITEMS_PER_PRODUCER = 20000
    BUFFER_SIZE = 1024

    results = {}
    for name, info in TARGETS.items():
        built = build_target(name, info)
        if not built:
            results[name] = {"skipped": True}
            continue

        cmd = f"{info['out']} {PRODUCER_COUNT} {CONSUMER_COUNT} {ITEMS_PER_PRODUCER} {BUFFER_SIZE} --quiet"
        print("Running:", cmd)
        res = run_and_measure(cmd)
        results[name] = res

    out_file = tmp_path / "benchmark_results.json"
    with open(out_file, "w") as f:
        json.dump(results, f, indent=2)

    # Also write to repo tests/results.json for README update
    with open("tests/results.json", "w") as f:
        json.dump(results, f, indent=2)

    # Basic assertion: at least one target ran
    ran = any(not v.get("skipped", False) for v in results.values())
    assert ran
