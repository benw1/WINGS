# Head-Node SlurmConsumer / PbsConsumer with Service Discovery

## Overview

The `SlurmConsumer` and `PbsConsumer` are asyncio TCP servers that batch `JobData` objects and submit them to the cluster scheduler (`sbatch` / `qsub`). They must run continuously on a stable node — not on compute nodes, which are subject to Slurm/PBS preemption, timeouts, and reboots.

This document describes the head-node architecture: how the consumer is started, discovered, and kept alive across pipeline runs.

---

## Why the Head Node?

| Location | Problem |
|---|---|
| Compute node (Slurm job) | All partitions have walltimes → consumer expires mid-pipeline; circular dependency |
| Compute node (background) | Node can be preempted or rebooted by the scheduler at any time |
| Head / login node | Always available; `sbatch` calls are lightweight and don't violate head-node policies |

The head/login node is the correct location. Discovery uses a shared-filesystem address file so all compute nodes can find it without any hardcoded IP addresses.

---

## Configuration

### Required: head node hostname

Set **one** of the following before running any pipeline:

**Option 1 — environment variable (recommended):**
```bash
export WPIPE_SLURM_HEAD_NODE=headnode01   # or whatever your login node is called
```

**Option 2 — INI config file** (`~/.slurmconsumer/config`):
```ini
[slurmconsumer]
head_node = headnode01
```

For PBS, use `WPIPE_PBS_HEAD_NODE` / `~/.pbsconsumer/config` with `[pbsconsumer]`.

If neither is set, `slurmconsumer("start")` raises a `RuntimeError` with instructions.

### Optional: port override

The default port is `8000` (Slurm) and `5000` (PBS). Override with:
```bash
export WPIPE_SLURM_PORT=18000
export WPIPE_PBS_PORT=15000
```

You can also pin the host (skips address file entirely):
```bash
export WPIPE_SLURM_HOST=headnode01
export WPIPE_SLURM_PORT=18000
```

---

## Address File

The consumer writes its location to a shared-filesystem file after successfully binding its port:

| Consumer | Address file |
|---|---|
| Slurm | `~/.slurmconsumer/server.address` |
| PBS | `~/.pbsconsumer/server.address` |

**Format:** `hostname:port:pid` (e.g. `headnode01:8000:47821`)

The file is written **after** the port is bound (no race window) and removed in the `finally` block on clean shutdown. A SIGKILL leaves a stale file; this is handled by stale-lock detection (see below).

### Address file lookup order (`read_address_file`)

1. `WPIPE_SLURM_HOST` + `WPIPE_SLURM_PORT` env vars → returns `(host, port, -1)`, skips file
2. `~/.slurmconsumer/server.address` → parses `hostname:port:pid`
3. Returns `None` if file is missing or malformed → caller falls back to `127.0.0.1:8000`

---

## Lifecycle: `slurmconsumer("start")`

```
1. checkSlurmConnection()
      └── 0 (connected) → already running, return
      └── non-zero → continue

2. _check_stale_lock("slurm")
      └── no address file → False, continue
      └── address file + TCP connects → raises RuntimeError ("already running")
      └── address file + connection refused/timeout → removes stale file, continue

3. _get_head_node("slurm")
      └── reads WPIPE_SLURM_HEAD_NODE or ~/.slurmconsumer/config
      └── neither set → RuntimeError with instructions

4. os.makedirs(~/.slurmconsumer, exist_ok=True)

5a. _is_on_head_node(head_node) == True
      └── subprocess.Popen(["nohup", sys.executable, "-m", "wpipe.scheduler.SlurmConsumer"],
                           start_new_session=True, all I/O → /dev/null)

5b. _is_on_head_node(head_node) == False
      └── ssh -o BatchMode=yes -o ConnectTimeout=10 <head_node>
              "nohup <sys.executable> -m wpipe.scheduler.SlurmConsumer </dev/null >/dev/null 2>&1 &"
      └── non-zero returncode → RuntimeError (check hostname, ssh-copy-id)

6. Poll checkSlurmConnection() every 0.5s, up to 30s total
      └── returns when connection == 0
      └── timeout → prints warning to check ~/.slurmconsumer/ logs
```

**SSH prerequisite:** Passwordless SSH from compute nodes to the head node must be configured:
```bash
ssh-copy-id headnode01
```

---

## Lifecycle: Consumer Process (`__main__`)

```
1. Configure logging → ~/.slurmconsumer/SlurmConsumerLog-MM-DD-YYYY.log
2. Redirect stdout/stderr into log via StreamToLogger
3. Register signal handlers (SIGTERM, SIGINT, SIGHUP, SIGQUIT, SIGABRT, etc.)
4. asyncio.get_event_loop() + loop.create_server("0.0.0.0", DEFAULT_PORT)
5. write_address_file("slurm", socket.gethostname(), actual_port, os.getpid())
6. loop.run_forever()
   ├── Receives pickled JobData → SlurmScheduler.submit(jobdata)
   ├── Receives "poisonpill" → loop.stop()
   └── call_later(30min) → periodicLog()
finally:
   ├── remove_address_file("slurm")   ← first action
   ├── server.close()
   └── loop.run_until_complete(server.wait_closed())
```

Signal handlers (SIGTERM, SIGINT) log context and re-raise, which unwinds through `finally` → address file is removed on clean signals. SIGKILL bypasses all handlers → stale address file, cleaned up on next start.

---

## Lifecycle: `slurmconsumer("stop")`

Sends `"poisonpill"` over TCP. The server's `data_received` calls `loop.stop()`, which exits `run_forever()` and runs the `finally` block (removes address file, closes server).

---

## Auto-Restart in `sendJobToSlurm`

If all send retries fail (default: 3 attempts with exponential back-off), `sendJobToSlurm` automatically calls `slurmconsumer("start")` and makes one final send attempt:

```
for attempt in range(max_retries):
    try: connect + sendall → return
    except ConnectionRefusedError: sleep(retry_delay * (attempt+1))

# retries exhausted
slurmconsumer("start")          ← triggers head-node start or SSH
connect + sendall → return      ← recovery succeeded

# if recovery also fails:
save_failed_job("slurm", jobData)   ← persists to ~/.slurmconsumer/failed_jobs/
```

Failed jobs are saved as JSON at `~/.slurmconsumer/failed_jobs/job_<timestamp>.json` for manual recovery.

---

## Edge Cases

| Scenario | Handling |
|---|---|
| SIGKILL leaves stale address file | `_check_stale_lock` probes TCP; connection refused → removes stale file |
| Two nodes race to start consumer | Second `bind()` fails with `Address already in use`; second process crashes before writing address file |
| SSH fails (wrong hostname, no key) | `subprocess.run(timeout=15)` → non-zero returncode → `RuntimeError` with instructions |
| NFS attribute caching delays | Discovery uses live TCP probe, not file `stat` |
| Default port taken on head node | Set `WPIPE_SLURM_PORT` to a free port |
| `WPIPE_SLURM_HEAD_NODE` not set | `RuntimeError` with exact instructions on what to set |

---

## Files Changed

| File | Change |
|---|---|
| `src/wpipe/scheduler/Utils.py` | Added `read_address_file`, `write_address_file`, `remove_address_file` |
| `src/wpipe/scheduler/SlurmConsumer.py` | Added `_get_slurm_address()`; updated `checkSlurmConnection`, `sendJobToSlurm`, `__main__`; removed hardcoded paths and 2-day auto-kill |
| `src/wpipe/scheduler/PbsConsumer.py` | Identical changes for PBS parity |
| `src/wpipe/scheduler/__init__.py` | Added `_get_head_node`, `_is_on_head_node`, `_check_stale_lock`; rewrote `slurmconsumer("start")` and `pbsconsumer("start")` with SSH path |

---

## Local Development Testing (no cluster required)

All tests run on a single machine using `localhost` as the head node.

### Prerequisites

```bash
# Install wpipe in editable mode (required for relative imports)
cd /path/to/WINGS
pip install -e .

# Set env vars
export WPIPE_SLURM_HEAD_NODE=$(hostname)
export WPIPE_SLURM_PORT=18000
mkdir -p ~/.slurmconsumer
```

### Test 1: Basic start / check / stop

```bash
python -c "from wpipe.scheduler import slurmconsumer; slurmconsumer('start')"
cat ~/.slurmconsumer/server.address   # hostname:18000:<pid>
python -c "from wpipe.scheduler import slurmconsumer; slurmconsumer('check')"   # 0
python -c "from wpipe.scheduler import slurmconsumer; slurmconsumer('stop')"
ls ~/.slurmconsumer/server.address    # should be gone
```

### Test 2: Duplicate start protection

```bash
python -c "from wpipe.scheduler import slurmconsumer; slurmconsumer('start')"
python -c "from wpipe.scheduler import slurmconsumer; slurmconsumer('start')"
# Second call prints: "SlurmConsumer is already running."
python -c "from wpipe.scheduler import slurmconsumer; slurmconsumer('stop')"
```

### Test 3: Stale lock detection (simulates SIGKILL)

```bash
python -c "from wpipe.scheduler import slurmconsumer; slurmconsumer('start')"
kill -9 $(cut -d: -f3 ~/.slurmconsumer/server.address)
sleep 1
python -c "from wpipe.scheduler import slurmconsumer; slurmconsumer('start')"
# Prints: "Stale address file found for Slurmconsumer at ...: Cleaning up ..."
# Then starts fresh consumer
python -c "from wpipe.scheduler import slurmconsumer; slurmconsumer('stop')"
```

### Test 4: SSH error path

```bash
export WPIPE_SLURM_HEAD_NODE=fakenode999
python -c "from wpipe.scheduler import slurmconsumer; slurmconsumer('start')"
# RuntimeError: SSH to fakenode999 failed (returncode 255)...
export WPIPE_SLURM_HEAD_NODE=$(hostname)
```

### Test 5: Missing head node config

```bash
unset WPIPE_SLURM_HEAD_NODE
rm -f ~/.slurmconsumer/config
python -c "from wpipe.scheduler import slurmconsumer; slurmconsumer('start')"
# RuntimeError: Head node not configured for slurmconsumer...
export WPIPE_SLURM_HEAD_NODE=$(hostname)
```

### Log inspection

```bash
tail -f ~/.slurmconsumer/SlurmConsumerLog-$(date +%m-%d-%Y).log
```

Key log lines:

| Message | Meaning |
|---|---|
| `Wrote address file: hostname:18000:PID` | Server bound successfully |
| `Removing address file: ...` | Clean shutdown (finally block ran) |
| `Connection was made ...` | Client connected |
| `Submitting job to scheduler ...` | Job passed to SlurmScheduler |
| `SlurmConsumer is still running ...` | Periodic heartbeat (every 30 min) |

### Force cleanup after failed test

```bash
pkill -f "wpipe.scheduler.SlurmConsumer" || echo "none running"
rm -f ~/.slurmconsumer/server.address
```
