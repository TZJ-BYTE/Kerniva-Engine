"""Join measured Go-log batches to interval-valued WSL and Windows disk counters."""
import argparse
from collections import Counter
import json
from pathlib import Path

from golog_batch_report import describe, records, require, window
from golog_host_clock import offset_bounds


# Explicit sensitivity assumption for relative host/guest monotonic clock rate.
DRIFT_PPM = 200
MS = 1_000_000


def read(path):
    return json.loads(path.read_bytes())


def clock_projection(clock):
    probes = []
    for key in ('pre', 'post'):
        burst = clock[key]
        require(all(r['guest_recv_ns'] >= r['guest_send_ns'] and r['host_after_ns'] >= r['host_before_ns']
                    for r in burst), 'reversed clock probe')
        require(len(burst) == 12 and list(offset_bounds(burst)) == clock[key + '_bounds_ns'],
                'missing or inconsistent clock probes')
        probes.extend(burst)
    require([r['token'] for r in probes] == list(range(24)), 'clock probe identity')
    require(all(a['guest_recv_ns'] <= b['guest_send_ns'] and a['host_after_ns'] <= b['host_before_ns']
                for a, b in zip(probes, probes[1:])), 'clock probe ordering')

    def project(host):
        def allowance(t):
            return (abs(host - t) * DRIFT_PPM + 999_999) // 1_000_000
        lo = max(r['host_after_ns'] - r['guest_recv_ns'] - allowance(r['host_after_ns']) for r in probes)
        hi = min(r['host_before_ns'] - r['guest_send_ns'] + allowance(r['host_before_ns']) for r in probes)
        require(lo <= hi, 'host and guest clocks exceed assumed drift envelope')
        return [host - hi, host - lo]
    return project


def host_buckets(directory):
    clock = read(directory / 'clocks.json')
    project = clock_projection(clock)
    meta = read(directory / 'disk/metadata.json')
    rows = list(records(directory / 'disk/samples.jsonl'))
    require(meta['exit_reason'] == 'stop_file' and len(rows) == meta['sample_count'] >= 2, 'incomplete host capture')
    mapping = meta['mapping']
    require(mapping['drive'] == 'F:' and mapping['instance'].split()[0] == str(mapping['disk_number']),
            'unexpected host disk')
    stamps = []
    for i, row in enumerate(rows):
        require(row['index'] == i and row['disk']['storage_device_number'] == mapping['disk_number'],
                'host sample order or disk identity')
        before, after = row['host_mono_before_ns'], row['host_mono_after_ns']
        require(before <= after and (i == 0 or before >= rows[i-1]['host_mono_after_ns']), 'host read order')
        stamps.append([project(before)[0], project(after)[1]])
    buckets = []
    invalid_cpu = 0
    for i, (a, b) in enumerate(zip(rows, rows[1:])):
        fields = ('writes', 'reads', 'read_bytes', 'write_bytes', 'write_time_100ns', 'read_time_100ns')
        disk = {k: b['disk'][k] - a['disk'][k] for k in fields}
        require(min(disk.values()) >= 0, 'host counters reset or wrapped')
        cpu = {k: v - a['cpu_100ns'][k] for k, v in b['cpu_100ns'].items()}
        total = cpu['kernel_including_idle'] + cpu['user']
        valid_cpu = min(cpu.values()) >= 0 and 0 <= cpu['idle'] <= total
        invalid_cpu += not valid_cpu
        buckets.append(dict(ix=i, start=stamps[i], end=stamps[i+1], **disk,
                            gap_ns=b['host_mono_before_ns']-a['host_mono_before_ns'],
                            queue_at_end=b['disk']['queue_depth'],
                            write_avg_ms=disk['write_time_100ns'] / 10000 / disk['writes'] if disk['writes'] else None,
                            cpu_busy_pct=100 * (total - cpu['idle']) / total if valid_cpu and total else None))
    if invalid_cpu:
        # Discard the entire short-interval CPU series, including possible rebounds.
        for bucket in buckets:
            bucket['cpu_busy_pct'] = None
    gaps = [b['host_mono_before_ns'] - a['host_mono_before_ns'] for a, b in zip(rows, rows[1:])]
    return buckets, dict(samples=len(rows), instance=mapping['instance'], period_ms=meta['interval_ms'],
                         gap_ns=describe(gaps), gaps_gt_2_periods=sum(v > 2 * meta['interval_ms'] * MS for v in gaps),
                         invalid_cpu_deltas=invalid_cpu, widest_endpoint_ns=max(hi-lo for lo, hi in stamps), drift_ppm=DRIFT_PPM,
                         pre_bounds_ns=clock['pre_bounds_ns'], post_bounds_ns=clock['post_bounds_ns'])


def guest_buckets(path):
    meta, *rows, footer = records(path)
    require(meta['record'] == 'meta' and footer['record'] == 'summary' and len(rows) == footer['sample_rows']
            and not footer['sources_failed'] and len(rows) >= 2, 'incomplete guest capture')
    stamps, disks = [], []
    for row in rows:
        src = row['source']['disk']
        disk = src['value']
        require((disk['major'], disk['minor']) == (meta['device']['major'], meta['device']['minor']),
                'guest device changed')
        stamps.append([src['start']['monotonic_before_ns'], src['end']['monotonic_after_ns']])
        require(stamps[-1][0] <= stamps[-1][1], 'guest observation bracket reversed')
        disks.append(disk['fields'])
    buckets = []
    for i, (a, b) in enumerate(zip(disks, disks[1:])):
        counters = {name: b[key]-a[key] for name, key in (
            ('flushes','flushes_completed'), ('flush_ms','flush_ms'), ('writes','writes_completed'),
            ('write_ms','write_ms'), ('sectors_written','sectors_written'))}
        require(min(counters.values()) >= 0 and stamps[i][0] <= stamps[i][1] <= stamps[i+1][0],
                'guest sample order or reset')
        buckets.append(dict(ix=i, start=stamps[i], end=stamps[i+1], **counters,
                            gap_ns=stamps[i+1][0]-stamps[i][0]))
    return buckets, dict(samples=len(rows), device=meta['device']['kernel_name'],
                         missed_slots=footer['missed_internal_slots'],
                         gap_ns=describe([b[0] - a[0] for a, b in zip(stamps, stamps[1:])]))


def match_buckets(buckets, start, end):
    possible = [b for b in buckets if b['start'][0] < end[1] and start[0] < b['end'][1]]
    definite = [b for b in possible if b['start'][1] < end[0] and start[1] < b['end'][0]]
    return definite, possible


def measured_batches(folder, joined):
    ports = {r['local_port'] for r in records(folder / 'client-trace.jsonl') if 'index' in r}
    batches = {}
    all_members = Counter()
    retained_sets = 0
    for row in records(folder / 'server-batches.jsonl'):
        batch = row.get('batch') or {}
        if not batch.get('id'):
            continue
        key = (batch['id'], batch['sync_start_ns'], batch['sync_end_ns'])
        all_members[key] += 1
        if row['local_port'] in ports and row['command'] == 'SET':
            batches[key] = batch
            retained_sets += 1
    # The periodic observer's SET can share a durable batch with the benchmark.
    require(len(batches) == joined['batches'] and retained_sets == joined['joined_sets']
            and all(all_members[key] == b['size'] for key, b in batches.items()), 'incomplete batch census')
    return sorted(batches.values(), key=lambda b: b['sync_start_ns'])


def public_bucket(bucket):
    return {k:v for k,v in bucket.items() if k not in ('start', 'end')}


def covered(buckets, start, end):
    return bool(buckets and buckets[0]['start'][1] <= start[0] and buckets[-1]['end'][0] >= end[1])


def whole_buckets(buckets, start, end):
    return [b for b in buckets if b['start'][0] >= start[1] and b['end'][1] <= end[0]]


def disk_baseline(buckets, start, end, fields):
    inside = whole_buckets(buckets, start, end)
    return {'whole_buckets':len(inside), **{
        field:describe([b[field] for b in inside if b[field] is not None]) for field in fields}}


def per_second(folder, origin, sources, batches, skew):
    seconds = [dict(second=i, slow_get=0, slow_set=0, before_send_ge_100ms=0,
                    exchange_ge_100ms=0, sync_starts_definitely_ge_10ms=0) for i in range(30)]
    for row in records(folder / 'client-trace.jsonl'):
        if 'index' not in row or row['completed_ns'] - row['planned_ns'] < 100 * MS:
            continue
        second = seconds[row['planned_ns'] // 10**9]
        second['slow_' + row['op'].lower()] += 1
        second['before_send_ge_100ms'] += row['send_start_ns'] - row['planned_ns'] >= 100 * MS
        second['exchange_ge_100ms'] += row['completed_ns'] - row['send_start_ns'] >= 100 * MS
    for batch in batches:
        if batch['sync_end_ns'] - batch['sync_start_ns'] < 10 * MS:
            continue
        earliest, latest = [(batch['sync_start_ns'] + v) // 10**9 for v in skew]
        if earliest == latest and 0 <= earliest < len(seconds):
            seconds[earliest]['sync_starts_definitely_ge_10ms'] += 1
    for i, second in enumerate(seconds):
        start = [v + i*10**9 for v in origin]
        end = [v + 10**9 for v in start]
        for source, buckets in sources.items():
            whole = whole_buckets(buckets, start, end)
            _, possible = match_buckets(buckets, start, end)
            fields = (('reads','writes','read_bytes','write_bytes','write_time_100ns','read_time_100ns')
                      if source == 'host' else ('writes','write_ms','flushes','flush_ms'))
            second[source] = {'whole_buckets':len(whole), 'possible_buckets':len(possible),
                              'covered':covered(buckets,start,end),
                              'max_gap_ms':max((b['gap_ns'] / MS for b in possible), default=None),
                              'whole_totals':{f:sum(b[f] for b in whole) for f in fields},
                              'possible_totals':{f:sum(b[f] for b in possible) for f in fields}}
    return seconds


def natural(folder, host_root):
    joined = window(folder)
    case, client, capture = (read(folder / f) for f in ('case.json', 'client.json', 'capture.json'))
    require(capture['pass'] and joined['gate_variant'] == 'C' and client['offered'] == 180000 and
            (client['clients'], client['read_percent'], client['keys'], client['target_rate'], client['network'])
            == (64,95,100000,6000,'gnet'), 'unexpected measured workload')
    guest, gq = guest_buckets(folder / 'guest-passive.jsonl')
    host, hq = [], None
    if capture['host_mode'] == 'on':
        host, hq = host_buckets(host_root / folder.name)
    else:
        require(capture['host_mode'] == 'off' and not (host_root / folder.name).exists(), 'invalid off control')
    alignment = case['measurement_alignment']
    require(alignment['projection_consistent'], 'guest measurement alignment inconsistent')
    origin = alignment['projected_start_monotonic_ns']
    skew = joined['client_minus_server_clock_bounds_ns']
    offset = [origin[i] + skew[i] for i in range(2)]
    batches = measured_batches(folder, joined)
    if host:
        require(host[0]['start'][1] <= origin[0] and host[-1]['end'][0] >=
                batches[-1]['sync_end_ns'] + offset[1], 'host capture does not cover measured batches')
    durations = [b['sync_end_ns'] - b['sync_start_ns'] for b in batches]
    selected = []
    sources = {'guest':guest}
    if host:
        sources['host'] = host
    outside = {key:0 for key in sources}
    for b, ns in zip(batches, durations):
        start = [v + b['sync_start_ns'] for v in offset]
        end = [v + b['sync_end_ns'] for v in offset]
        for key, buckets in sources.items():
            outside[key] += not covered(buckets, start, end)
        if ns < 10 * MS:
            continue
        event = dict(batch=b['id'], size=b['size'], sync_ns=ns, start=start, end=end)
        for key, buckets in sources.items():
            definite, possible = match_buckets(buckets, start, end)
            event[key] = dict(covered=covered(buckets,start,end),
                              gaps_gt_80ms=[r['ix'] for r in possible if r['gap_ns'] > 80 * MS],
                              whole=[r['ix'] for r in whole_buckets(possible,start,end)],
                              definite=[public_bucket(row) for row in definite],
                              possible=[row['ix'] for row in possible])
        selected.append(event)
    stop = alignment['projected_offer_end_monotonic_ns']
    gq['measurement'] = disk_baseline(guest, origin, stop, ('flushes','flush_ms','write_ms','writes'))
    if hq:
        hq['measurement'] = disk_baseline(host, origin, stop, ('writes','write_avg_ms','queue_at_end','cpu_busy_pct'))
    seconds = per_second(folder, origin, sources, batches, skew)
    require(all(sum(s['slow_' + k] for s in seconds) == joined['slow'][k] for k in ('get','set')),
            'per-second slow request census mismatch')
    return dict(folder=folder.name, host_mode=capture['host_mode'], requests=client['offered'],
                failures=client['failures'], window_success_rate=client['window_success_rate'],
                get_set_p99_us=joined['p99_us'], slow_requests=joined['slow'],
                joined_sets=joined['joined_sets'], batches=len(batches), sync_ns=describe(durations),
                max_queue_ns=joined['phases_ns']['batch_queue']['max'], sync_ge_10ms=len(selected),
                sync_ge_50ms=sum(v >= 50 * MS for v in durations), sync_ge_100ms=sum(v >= 100 * MS for v in durations),
                server_origin_width_ns=offset[1]-offset[0], batches_outside_definite_coverage=outside, guest=gq, host=hq,
                longest_slow_set=joined['longest_exchange'], slow_batches=selected, seconds=seconds)


def compact(result):
    windows = []
    for w in result:
        out = {k:w[k] for k in ('folder','host_mode','requests','failures','window_success_rate','get_set_p99_us',
            'slow_requests','joined_sets','batches','sync_ns','max_queue_ns','sync_ge_10ms','sync_ge_50ms',
            'sync_ge_100ms','batches_outside_definite_coverage')}
        out['guest_samples'] = w['guest']['samples']
        out['guest_missed_slots'] = w['guest']['missed_slots']
        out['max_guest_gap_ns'] = w['guest']['gap_ns']['max']
        if w['host']:
            out['host_samples'] = w['host']['samples']
            out['max_host_gap_ns'] = w['host']['gap_ns']['max']
            out['invalid_host_cpu_deltas'] = w['host']['invalid_cpu_deltas']
        out['nonexclusive_slow_counts'] = {k:sum(s[k] for s in w['seconds'])
            for k in ('before_send_ge_100ms','exchange_ge_100ms')}
        long = [b for b in w['slow_batches'] if b['sync_ns'] >= 100 * MS]
        if long:
            out['inside_syncs_ge_100ms'] = {}
            for src, fields in [('host',('reads','writes','write_bytes')), ('guest',('writes','flushes','flush_ms'))]:
                if src == 'host' and not w['host']:
                    continue
                whole = {r['ix']:r for event in long for r in event[src]['definite'] if r['ix'] in event[src]['whole']}
                out['inside_syncs_ge_100ms'][src] = {'distinct_whole_buckets':len(whole),
                    'batches_with_whole_buckets':sum(bool(e[src]['whole']) for e in long),
                    **{k:sum(r[k] for r in whole.values()) for k in fields}}
        windows.append(out)
    return {'schema':1, 'host_guest_drift_limit_ppm':DRIFT_PPM, 'windows':windows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--raw', required=True, type=Path)
    parser.add_argument('--host-root', required=True, type=Path)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--summary', type=Path, help='also write the compact window and whole-bucket totals')
    args = parser.parse_args()
    driver = read(args.raw / 'run.json')
    require(driver['driver_exit_code'] == 0 and driver['kind'] == 'natural' and driver['group_max'] == 8 and
            [w['folder'] for w in driver['windows']] == [f'{i:02d}-r95-q6000-A' for i in range(1,9)],
            'incomplete or repeated natural windows')
    result = [natural(args.raw / w['folder'], args.host_root) for w in driver['windows']]
    require([r['host_mode'] for r in result] == ['off'] + ['on']*6 + ['off'], 'host order mismatch')
    args.out.write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + '\n', encoding='utf-8')
    if args.summary:
        args.summary.write_text(json.dumps(compact(result), indent=2, allow_nan=False) + '\n', encoding='utf-8')


if __name__ == '__main__':
    main()
