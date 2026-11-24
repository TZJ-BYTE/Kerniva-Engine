# GoLog idle pull allocation diagnostics

Run from Linux with Go, Python 3.12+, Git, and an ext4 output location. Use a passed ordinary fixture containing a converged 16-database follower and its source data. The linked Rust library must be the same one used to generate the fixture.

```bash
python3 scripts/diagnostics/golog_idle_polling/run.py \
  --root /path/on/ext4/idle-baseline \
  --origin /path/on/ext4/ordinary-passed-window \
  --library /path/to/frozen/libkerniva_engine.so \
  --revision 93787a2
```

For an uncommitted candidate, use `--checkout /path/to/candidate` in place of `--revision` and a fresh output path outside that checkout. The copy skips build outputs, runtime data, logs, and local dependency directories. Pass `--source-binary /path/on/ext4/idle-baseline/idle-diagnostic` to use the same instrumented source binary across trials. With no `--window` arguments, the runner measures each interval in exact and profile mode. Repeat `--window 1s exact` or `--window 100ms profile` to select a different order. Run one experiment at a time.

The runner copies the fixture and sources to ext4 and applies two diagnostic hooks in the copy. The follower uses `OpenFollowerWithOptions`, `Run`, and its normal HTTP transport. A separate process serves the actual source handler and counts accepted connections and pull requests. Each follower worker completes three warm-up polls and parks before the next poll. A second barrier parks all workers after the requested duration. The elapsed time includes the wait for the last worker to reach that barrier. Each successful unchanged response is counted after validation and end-of-body handling. The runner verifies zero poll errors, unchanged status and CURRENT for all 16 databases, and agreement between the source request count and the follower's completed pulls. The source connection count includes warm-up and the measured window.

Use the exact-mode `result.json` for `runtime.MemStats.TotalAlloc` and bytes per pull. Profile mode uses an independent process and a 1 KiB sampling rate. It takes allocs profiles after two GC cycles while workers are parked at each boundary, differences the snapshots with `pprof -base`, and excludes the profile writer from the workload reports. The filtered reports preserve both application and HTTP transport allocations. `alloc.bufio.txt` shows the call stacks behind identically named buffer allocations. The sampled profile estimates stack attribution. The exact mode gives the allocation numerator and completed-pull denominator.

`build.json` records the selected sources and SHA-256 hashes for the original and instrumented follower files, diagnostic implementation, binaries, Rust library, and origin result. Logs, results, and profile files stay under `--root`.

## Phase-aligned marker timing

Build the diagnostic CLI from the baseline and candidate into separate ext4 directories.

```bash
python3 scripts/diagnostics/golog_idle_polling/build_phase.py \
  --root /path/on/ext4/phase-baseline --revision 93787a2
python3 scripts/diagnostics/golog_idle_polling/build_phase.py \
  --root /path/on/ext4/phase-candidate --checkout /path/to/candidate
```

Set `KERNIVA_GOLOG_POLL_PHASE_TRACE` to a new JSONL path and `KERNIVA_GOLOG_POLL_PHASE_DBS` to a comma-separated list of databases when launching the diagnostic CLI in follower mode. Each row includes the database, stage, a Linux `CLOCK_MONOTONIC` timestamp in nanoseconds, and `emitted_ns` from the writer immediately before encoding. Compare the driver observation time with both clocks to account for dispatch and file observation delay.

| Stage | Observation |
| --- | --- |
| `poll_no_change` | An unchanged pull completed and closed the response body. The worker is about to return to its interval timer. `after` and `last` both give the current sequence. |
| `pull_request` | The follower is about to call `http.Client.Do`. `after` gives the requested cursor. |
| `manifest_validated` | The follower validated the manifest and source identity. Includes `after`, `last`, `files`, `snapshot`, and `rebase`. |
| `current_durable` | The follower synced the directory after publishing `CURRENT`. `last` gives the durable sequence. |
| `view_published` | The new read view is visible. Includes the prior and updated sequences in `after` and `last`. |
| `poll_error` | The poll returned an error during normal operation. Includes `error`. |
| `trace_closed` | The writer drained the queue at graceful shutdown. `dropped` gives the number of events discarded by the nonblocking queue. A complete measurement has a terminal row with zero drops. |

The producer timestamps events and sends them through a bounded queue. A separate goroutine encodes and writes the rows for live observation. The same instrumentation is applied to both CLI builds, and `build.json` records the matching probe hashes.
