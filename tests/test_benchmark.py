import json
import os
import shlex
import subprocess
import sys
import time
import psutil

BUILD_DIR = "build"
os.makedirs(BUILD_DIR, exist_ok=True)

def get_pkg_config(name):
    proc = subprocess.run(
        ["pkg-config", "--cflags", "--libs", "--static", name],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return proc.stdout.strip() if proc.returncode == 0 else None

FOLLY_PKG_CONFIG = get_pkg_config("libfolly")

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
        "src": "FollyQueue/producer_consumer_folly.cpp",
        "cflags": f"{FOLLY_PKG_CONFIG} -lfmt" if FOLLY_PKG_CONFIG else "",
        "out": f"{BUILD_DIR}/mpmc_demo",
    },
}


def build_target(name, info):
    cmd = f"g++ -std=c++17 -O2 -pthread {info['src']} {info['cflags']} -o {info['out']}"
    print("Building:", cmd)
    proc = subprocess.run(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        print(f"Build failed for {name}:", proc.stderr.decode())
        return False
    return True


def run_and_measure(cmd, timeout=None):
    p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    proc = psutil.Process(p.pid)
    peak_rss = 0
    stdout = ""
    stderr = ""
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
        stdout, stderr = p.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        p.kill()
        stdout, stderr = p.communicate()
    except Exception:
        p.kill()
        raise

    result = {
        "returncode": p.returncode,
        "peak_rss_bytes": peak_rss,
    }

    if stdout:
        try:
            parsed = json.loads(stdout.strip())
            result.update(parsed)
        except json.JSONDecodeError:
            result["stdout"] = stdout.strip()
            result["stderr"] = stderr.strip()

    return result


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
