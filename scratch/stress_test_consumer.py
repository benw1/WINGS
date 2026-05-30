#!/usr/bin/env python
"""
Stress test for SlurmConsumer responsiveness under burst job submissions.

Verifies that checkSlurmConnection() returns 0 throughout burst submissions —
the condition that previously caused "connection refused" errors.

Usage:
    cd /home/tristan/sandbox/WINGS
    export WPIPE_SLURM_HEAD_NODE=$(hostname)
    export WPIPE_ENGINEURL="sqlite:////tmp/wpipe_stress.db"
    python scratch/stress_test_consumer.py
"""

import os
import sys
import socket
import pickle
import time
import threading

# ---------------------------------------------------------------------------
# Dev environment setup — must happen before any wpipe imports
# ---------------------------------------------------------------------------
os.environ.setdefault("WPIPE_ENGINEURL", "sqlite:////tmp/wpipe_stress.db")
os.environ.setdefault("WPIPE_SLURM_HEAD_NODE", socket.gethostname())
os.makedirs("/tmp/stress_config", exist_ok=True)
os.makedirs("/tmp/stress_pipe", exist_ok=True)

# ---------------------------------------------------------------------------
# Imports (after env is set)
# ---------------------------------------------------------------------------
from wpipe.scheduler.JobData import JobData
from wpipe.scheduler.SlurmConsumer import checkSlurmConnection, _get_slurm_address
from wpipe.scheduler import slurmconsumer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_fake_jobdata(job_id: int) -> JobData:
    """Bypass JobData.__init__ (which requires a live wpipe session)."""
    jd = object.__new__(JobData)
    jd._task_name = "stress_task"
    jd._pipeline_pipe_root = "/tmp/stress_pipe"
    jd._pipeline_config_root = "/tmp/stress_config"
    jd._task_executable = "/bin/echo"
    jd._pipeline_id = 1
    jd._pipeline_username = "stress_user"
    jd._job_id = job_id
    jd._verbose = False
    jd._job_time = None
    jd._node_model = "has"
    jd._walltime = "01:00:00"
    jd._memory = "1G"
    jd._slurm_partition = "compute"
    jd._slurm_account = "test"
    jd._job_openmp = False
    jd._job_condaenv = ""
    jd._ncpus = "1"
    return jd


def send_raw(host: str, port: int, jobdata: JobData):
    """Send a JobData object via raw TCP socket (no retry logic overhead)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(pickle.dumps(jobdata))


def check_latency() -> tuple:
    """Returns (result_code, elapsed_ms)."""
    t0 = time.monotonic()
    result = checkSlurmConnection()
    return result, (time.monotonic() - t0) * 1000


def wait_for_consumer(timeout: float = 30.0) -> bool:
    """Poll until consumer responds (returns 0) or timeout expires."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if checkSlurmConnection() == 0:
            return True
        time.sleep(0.25)
    return False


# ---------------------------------------------------------------------------
# Test phases
# ---------------------------------------------------------------------------

def run_phase_1(host: str, port: int) -> dict:
    """
    Phase 1: Single burst — 50 jobs sequentially (5 full batches of 10).
    After each job, immediately call checkSlurmConnection().
    """
    print("\n[Phase 1] Single burst: 50 jobs sequentially ...")
    failures = 0
    max_latency_ms = 0.0
    job_id_base = 1000

    for i in range(50):
        jd = make_fake_jobdata(job_id_base + i)
        try:
            send_raw(host, port, jd)
        except Exception as e:
            print("  [Phase 1] send_raw failed at job {}: {}".format(i, e))
            failures += 1
            continue

        code, latency = check_latency()
        if code != 0:
            print("  [Phase 1] checkSlurmConnection() = {} at job {}".format(code, i))
            failures += 1
        if latency > max_latency_ms:
            max_latency_ms = latency

    print("  failures={}, max_latency={:.1f}ms".format(failures, max_latency_ms))
    return {"failures": failures, "max_latency_ms": max_latency_ms}


def run_phase_2(host: str, port: int) -> dict:
    """
    Phase 2: Concurrent senders — 5 threads each sending 10 jobs simultaneously.
    Tests the _lock on the schedulers list under contention.
    """
    print("\n[Phase 2] Concurrent senders: 5 threads × 10 jobs ...")
    failures_lock = threading.Lock()
    failures = [0]
    job_id_base = 2000

    def sender_thread(thread_idx: int):
        for j in range(10):
            job_id = job_id_base + thread_idx * 10 + j
            jd = make_fake_jobdata(job_id)
            try:
                send_raw(host, port, jd)
            except Exception as e:
                print("  [Phase 2] Thread {} send failed: {}".format(thread_idx, e))
                with failures_lock:
                    failures[0] += 1

    threads = [threading.Thread(target=sender_thread, args=(t,)) for t in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Verify consumer still responds after concurrent load
    code, latency = check_latency()
    if code != 0:
        print("  [Phase 2] Consumer unresponsive after concurrent burst: code={}".format(code))
        failures[0] += 1

    print("  failures={}, final_latency={:.1f}ms".format(failures[0], latency))
    return {"failures": failures[0], "max_latency_ms": latency}


def run_phase_3(host: str, port: int) -> dict:
    """
    Phase 3: Sustained rate — 100 jobs at 20ms spacing, connection check every 5 sends.
    Simulates real-world pipeline submission load.
    """
    print("\n[Phase 3] Sustained rate: 100 jobs at 20ms spacing ...")
    failures = 0
    max_latency_ms = 0.0
    job_id_base = 3000

    for i in range(100):
        jd = make_fake_jobdata(job_id_base + i)
        try:
            send_raw(host, port, jd)
        except Exception as e:
            print("  [Phase 3] send_raw failed at job {}: {}".format(i, e))
            failures += 1

        if (i + 1) % 5 == 0:
            code, latency = check_latency()
            if code != 0:
                print("  [Phase 3] checkSlurmConnection() = {} at job {}".format(code, i))
                failures += 1
            if latency > max_latency_ms:
                max_latency_ms = latency

        time.sleep(0.020)

    print("  failures={}, max_latency={:.1f}ms".format(failures, max_latency_ms))
    return {"failures": failures, "max_latency_ms": max_latency_ms}


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_report(results: dict) -> bool:
    """Print summary and return True if all phases passed."""
    print("\n" + "=" * 60)
    print("STRESS TEST REPORT")
    print("=" * 60)

    MAX_LATENCY_MS = 500.0
    overall_pass = True

    for phase, data in sorted(results.items()):
        f = data["failures"]
        lat = data["max_latency_ms"]
        phase_pass = (f == 0) and (lat <= MAX_LATENCY_MS)
        status = "PASS" if phase_pass else "FAIL"
        print("  {:8s}  failures={:3d}  max_latency={:6.1f}ms  [{}]".format(
            phase, f, lat, status
        ))
        if not phase_pass:
            overall_pass = False

    print("=" * 60)
    return overall_pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Starting SlurmConsumer stress test ...")
    print("  WPIPE_SLURM_HEAD_NODE =", os.environ.get("WPIPE_SLURM_HEAD_NODE"))
    print("  WPIPE_ENGINEURL       =", os.environ.get("WPIPE_ENGINEURL"))

    # Start consumer
    print("\nStarting SlurmConsumer ...")
    slurmconsumer("start")

    # Wait for consumer to be ready
    print("Waiting for consumer to become available ...")
    if not wait_for_consumer(timeout=30.0):
        print("ERROR: Consumer did not come up within 30s. Aborting.")
        sys.exit(1)

    host, port = _get_slurm_address()
    print("Consumer ready at {}:{}".format(host, port))

    results = {}

    try:
        results["phase_1"] = run_phase_1(host, port)
        results["phase_2"] = run_phase_2(host, port)
        results["phase_3"] = run_phase_3(host, port)
    finally:
        # Always stop the consumer
        print("\nStopping SlurmConsumer ...")
        slurmconsumer("stop")
        # Brief wait for clean shutdown
        time.sleep(1.0)

    overall_pass = print_report(results)

    if overall_pass:
        print("\nPASS — consumer remained responsive across all 3 phases.")
        sys.exit(0)
    else:
        print("\nFAIL — one or more phases exceeded thresholds.")
        sys.exit(1)


if __name__ == "__main__":
    main()
