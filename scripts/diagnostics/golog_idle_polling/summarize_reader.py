#!/usr/bin/env python3
"""Recompute response-reader comparison metrics from the raw WSL evidence."""

import argparse
from collections import defaultdict
from hashlib import file_digest, sha256
import json
from pathlib import Path
import re
from statistics import median


MS = 1_000_000
INTERVALS = ('1s', '100ms')
SIDES = ('A1', 'B1', 'B2', 'A2', 'A3', 'B3')
PHASE_RUNS = ('phase', 'phase-db0', 'phase-db7')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def distribution(values):
    return {'min': min(values), 'median': median(values), 'max': max(values)}


def bounds_distribution(bounds):
    lower, upper = zip(*bounds)
    return {'lower': distribution(lower), 'upper': distribution(upper)}


def exact(root):
    runs = {}
    profiles = {}
    missing_companions = []
    for side in SIDES:
        for path in sorted((root / side).glob('*-*/result.json')):
            result = json.loads(path.read_text())
            identity = (side, result['interval'])
            mode = result['mode']
            require(mode in {'exact', 'profile'}, f'{path}: unexpected idle mode')
            companions = ('alloc.bufio.txt',) if mode == 'profile' else ('follower.log', 'source.log')
            missing = [path.with_name(name).relative_to(root).as_posix()
                       for name in companions if not path.with_name(name).is_file()]
            if missing:
                missing_companions.extend(missing)
                continue
            if mode == 'profile':
                profiles[identity] = profile(path.parent)
                continue
            follower_lines = path.with_name('follower.log').read_text().splitlines()
            follower = json.loads(follower_lines[0])
            source_log = path.with_name('source.log').read_text()
            counts = re.search(r'^source_pulls=(\d+) source_new_connections=(\d+)$', source_log, re.M)
            require(counts is not None and 'PASS' in follower_lines and 'PASS' in source_log.splitlines(),
                    f'{path}: missing pull counts or successful exit')
            pulls, connections = map(int, counts.groups())
            success = follower['completed_no_change']
            warmup = follower['warmup_no_change']
            per_db = follower['no_change_per_db']
            require(len(per_db) == 16 and sum(per_db) == success and success + warmup == pulls,
                    f'{path}: inconsistent pull counts')
            require(all(result.get(key) == value for key, value in follower.items())
                    and (result['source_pulls'], result['source_new_connections']) == (pulls, connections),
                    f'{path}: result and logs disagree')
            require(identity not in runs, f'{path}: duplicate exact window')
            runs[identity] = {**result, 'path': path.relative_to(root).as_posix(),
                              'alloc_bytes_per_pull': follower['total_alloc_bytes'] / success}

    def pairs(orders):
        output = []
        for order in orders:
            baseline = next(side for side in order if side.startswith('A'))
            candidate = next(side for side in order if side.startswith('B'))
            for interval in INTERVALS:
                a, b = runs.get((baseline, interval)), runs.get((candidate, interval))
                if not a or not b:
                    continue
                a_bytes, b_bytes = (row['alloc_bytes_per_pull'] for row in (a, b))
                output.append({'order': order, 'interval': interval, 'baseline': a, 'candidate': b,
                               'candidate_minus_baseline_bytes_per_pull': b_bytes - a_bytes,
                               'reduction_percent': (a_bytes - b_bytes) / a_bytes * 100})
        return output

    return {'main_pairs': pairs((('B2', 'A2'), ('A3', 'B3'))),
            'initial_pairs': pairs((('A1', 'B1'),)),
            'missing_exact_windows': [{'run': side, 'interval': interval}
                                      for side in SIDES for interval in INTERVALS if (side, interval) not in runs],
            'missing_profile_windows': [{'run': side, 'interval': interval}
                                        for side in ('A1', 'B1') for interval in INTERVALS
                                        if (side, interval) not in profiles],
            'missing_companions': missing_companions,
            'profiles': [{'run': side, 'interval': interval, 'application_reader_stacks': stacks,
                          'application_reader_stack_count': len(stacks),
                          'no_application_reader_samples': not stacks}
                         for (side, interval), stacks in profiles.items()]}


def profile(directory):
    stacks = []
    report = (directory / 'alloc.bufio.txt').read_text()
    for block in re.split(r'^-+\+-+$', report, flags=re.M):
        if 'bufio.NewReader' not in block or 'gologreplica.(*Follower).pullOnce' not in block:
            continue
        sampled = re.search(r'^\s*([\d.]+\s*\w+)\s+bufio\.NewReader', block, re.M)
        require(sampled is not None, f'{directory}: unreadable application allocation sample')
        object_size = re.search(r'^\s*bytes:\s*(\S+)', block, re.M)
        stacks.append({'alloc_space_rounded': sampled[1],
                       'object_size_rounded': object_size[1] if object_size else None})
    return stacks


def marker_bounds(probe, source_lower, source_upper):
    last_negative = probe.get('last_negative')
    lower = max(0, last_negative['start_ns'] - source_upper) / MS if last_negative else 0
    return [lower, (probe['first_positive']['end_ns'] - source_lower) / MS]


def ordinary(root):
    plan_path = root / 'ordinary' / 'result.json'
    plan = json.loads(plan_path.read_text()) if plan_path.is_file() else {}
    windows = []
    groups = defaultdict(list)
    for path in sorted((root / 'ordinary').glob('*/result.json')):
        row = json.loads(path.read_text())
        window = {'path': path.relative_to(root).as_posix(), 'passed': row.get('passed', False),
                  'binary': row['binary'], 'interval': row['interval'], 'markers': []}
        windows.append(window)
        if not window['passed']:
            continue
        first, last = (row['idle'][point] for point in ('first', 'last'))
        before, after = first['memory'], last['memory']
        seconds = (after['monotonic_ns'] - before['monotonic_ns']) / 1e9
        allocated = after['total_alloc_bytes'] - before['total_alloc_bytes']
        cpu_ticks = last['proc']['cpu_ticks'] - first['proc']['cpu_ticks']
        window.update(elapsed_seconds=seconds, total_alloc_bytes=allocated,
                      alloc_bytes_per_second=allocated / seconds,
                      alloc_mib_per_second=allocated / (1 << 20) / seconds,
                      gc_cycles=after['num_gc'] - before['num_gc'],
                      gc_cycles_per_second=(after['num_gc'] - before['num_gc']) / seconds,
                      cpu_ms_per_second=cpu_ticks / row['idle']['ticks_per_second'] * 1000 / seconds,
                      connection_samples=[point['connections'] for point in (first, last)])
        for marker in row['markers']:
            probes = marker['probes']
            lower = probes['sealed_file']['last_negative']['start_ns']
            upper = probes['source_pull']['first_positive']['end_ns']
            window['markers'].append({**{key: marker[key] for key in ('db', 'ordinal', 'before', 'after')},
                                      'source_bound_width_ms': (upper - lower) / MS,
                                      **{name: marker_bounds(probes[name], lower, upper)
                                         for name in ('b_current', 'b_get')}})
        groups[row['interval'], row['binary']].append(window)

    comparisons = []
    for (interval, binary), group in groups.items():
        markers = [marker for window in group for marker in window['markers']]
        comparisons.append({'interval': interval, 'binary': binary, 'windows': len(group),
                            'markers': len(markers), 'alloc_bytes_per_second': distribution(
                                [window['alloc_bytes_per_second'] for window in group]),
                            'gc_cycles': [window['gc_cycles'] for window in group],
                            'connection_samples': [window['connection_samples'] for window in group],
                            'source_bound_width_ms': distribution(
                                [marker['source_bound_width_ms'] for marker in markers]),
                            **{name: bounds_distribution([marker[name] for marker in markers])
                               for name in ('b_current', 'b_get')}})
    seen_windows = {int(Path(window['path']).parent.name.split('-')[0]) for window in windows}
    return {'windows': windows, 'groups': comparisons, 'passed': plan.get('passed'),
            'missing_window_numbers': [number for number in range(1, 9) if number not in seen_windows],
            'plan_result_present': plan_path.is_file(),
            'passed_windows': sum(window['passed'] for window in windows),
            'marker_count': sum(len(window['markers']) for window in windows)}


def phase(root, name):
    directory = root / name
    plan_path = directory / 'result.json'
    if not plan_path.is_file():
        return {'state': 'missing_plan_result' if directory.is_dir() or name == 'phase' else 'not_recorded',
                'attempt_results_present': [path.relative_to(root).as_posix()
                                            for path in sorted(directory.glob('*-attempt*/result.json'))]}
    plan = json.loads(plan_path.read_text())
    tolerance = plan['source_phase_matching_tolerance_ms']
    max_bound = plan['max_source_bound_ms']
    exclusions = {Path(row['result']).parent.name: row['reason'] for row in plan['exclusions']}
    attempted = []
    rounds = {}
    phases = defaultdict(list)
    install_times = defaultdict(lambda: defaultdict(list))
    for path in sorted(directory.glob('*-attempt*/result.json')):
        row = json.loads(path.read_text())
        round_number = int(path.parent.name.split('-')[0])
        window = {'path': path.relative_to(root).as_posix(), 'round': round_number,
                  'binary': row['binary'], 'passed': row.get('passed', False),
                  'exclusion': exclusions.get(path.parent.name)}
        attempted.append(window)
        if not window['passed']:
            window['error'] = row.get('error')
            window['trace_dropped_events'] = row.get('trace_close', {}).get('dropped')
            continue
        require(round_number not in rounds, f'{path}: repeated eligible round')
        require(row['binary'] == plan['plan'][round_number - 1], f'{path}: unexpected binary for round')
        require([marker['db'] for marker in row['markers']] == [db for db, _ in plan['phases']],
                f'{path}: unexpected marker DBs')
        require(row['trace_close']['dropped'] == 0, f'{path}: dropped trace events')
        selected = {**window, 'markers': []}
        window['trace_dropped_events'] = row['trace_close']['dropped']
        selected['trace_dropped_events'] = window['trace_dropped_events']
        for marker in row['markers']:
            t0, t1, durable, view = (marker[key]['monotonic_ns'] for key in ('t0', 't1', 'durable', 'view'))
            lower, upper = marker['source_bounds_ns']
            db = marker['db']
            target = dict(plan['phases'])[db]
            midpoint = ((lower - t0) + (upper - t0)) / (2 * MS)
            previous = phases[db]
            require(upper < t1 <= durable <= view and (upper - lower) / MS <= max_bound
                    and all(abs(midpoint - reference) <= tolerance for reference in (target, *previous)),
                    f'{path}: ineligible phase DB {db}')
            previous.append(midpoint)
            install_times[db][row['binary']].append((view - t1) / MS)
            first_pull = sum(event['stage'] == 'pull_request' and event['monotonic_ns'] <= view
                             for event in marker['events']) == 1
            require(first_pull, f'{path}: DB {db} did not publish on its first post-T0 pull')
            selected['markers'].append({'db': db, 'target_ms': target,
                                      'source_phase_bounds_ms': [(lower - t0) / MS, (upper - t0) / MS],
                                      'source_phase_midpoint_ms': midpoint,
                                      'schedule_ms': (t1 - t0) / MS,
                                      'source_to_request_bounds_ms': [(t1 - upper) / MS, (t1 - lower) / MS],
                                      'request_to_durable_ms': (durable - t1) / MS,
                                      'request_to_view_ms': (view - t1) / MS,
                                      'request_to_get_bounds_ms': marker_bounds(marker['probes']['b_get'], t1, t1),
                                      'source_to_get_bounds_ms': marker_bounds(marker['probes']['b_get'], lower, upper),
                                      'first_post_t0_pull_published': first_pull})
        rounds[round_number] = selected
    grouped = {db: {'source_phase_midpoint_spread_ms': max(values) - min(values),
                    'request_to_view_ms': {label: distribution(times)
                                           for label, times in install_times[db].items()}}
               for db, values in phases.items()}
    for group in grouped.values():
        times = group['request_to_view_ms']
        if 'candidate' in times and 'baseline' in times:
            group['candidate_minus_baseline_median_view_install_ms'] = (
                times['candidate']['median'] - times['baseline']['median'])
    return {'state': 'recorded', 'passed': plan.get('passed', False), 'attempts': attempted,
            'missing_eligible_rounds': [number for number in range(1, len(plan['plan']) + 1)
                                        if number not in rounds],
            'first_eligible_rounds': list(rounds.values()), 'source_phase_matching_tolerance_ms': tolerance,
            'max_source_bound_ms': max_bound, 'excluded_attempt_count': len(exclusions),
            'groups_by_db': grouped}


def loaded_window(path, root):
    row = json.loads(path.read_text())
    result = {'path': path.relative_to(root).as_posix(), 'passed': row.get('passed', False),
              'limit': row.get('limit'), 'fault_enabled': row.get('fault_enabled'),
              'error': row.get('error'), 'relay_errors': row.get('relay_errors'),
              'relay_shutdown_errors': row.get('relay_shutdown_errors'),
              'fixture_source': row.get('fixture', {}).get('source'),
              'sampled_bulk_sha256': row.get('sampled_bulk_sha256'),
              'final': row.get('final'), 'fault': row.get('fault'), 'markers': []}
    measurement = row.get('measurement')
    if measurement:
        result['measurement'] = {**measurement, 'convergence_ms':
                                 (measurement['finish_ns'] - measurement['start_ns']) / MS}
        samples = row['samples']
        result['measurement']['peak_partial_on_disk'] = max(len(sample['partial']) for sample in samples)
        result['measurement']['rss_hwm_kib'] = max(sample['hwm_kib'] for sample in
                                                   [*samples, measurement['final_proc']])
        events = sorted((attempt[edge], change) for attempt in row['attempts']
                        for edge, change in (('first_ns', 1), ('end_ns', -1)) if edge in attempt)
        active = peak = 0
        for _, change in events:
            active += change
            peak = max(peak, active)
        result['measurement']['peak_payload_streams'] = peak
    if 'shared' in row:
        result['link'] = {key: row['shared'][key] for key in
                          ('achieved_payload_bytes_per_second', 'max_observed_early_bytes')}
    result['transfer_outcomes'] = dict.fromkeys(('complete', 'interrupted'), 0)
    for attempt in row.get('attempts', []):
        outcome = attempt.get('outcome', 'unknown')
        result['transfer_outcomes'][outcome] = result['transfer_outcomes'].get(outcome, 0) + 1
    for marker in row.get('markers', []):
        witness = marker['witness']
        source = witness['source']
        lower = source.get('last_negative', {}).get('start_ns', marker['write_start_ns'])
        upper = source['first_positive']['end_ns']
        result['markers'].append({
            'sequence': marker['sequence'], 'value': marker['value'],
            'progress_target': marker['progress_target'],
            'paced_snapshot_bytes_at_write': marker['paced_snapshot_bytes_at_write'],
            'attempts_at_write': marker.get('attempts_at_write'),
            'attempts_at_witness': marker.get('attempts_at_witness'),
            'intervening_gaps_ns': marker.get('intervening_gaps_ns'),
            'witness': witness, 'source_availability_bounds_ns': [lower, upper],
            'source_to_witness_bounds_ms': {name: marker_bounds(witness[name], lower, upper)
                                            for name in ('a_get', 'b_get', 'b_current')},
        })
    return result


def loaded(root):
    schedule = ('01-A', '02-B', '03-B', '04-A')
    directory = root / 'loaded'
    windows = {}
    attempts = {}
    for name in (*schedule, 'baseline-fault', 'candidate-fault'):
        cap = 4 if name.endswith('-fault') else 0
        initial = directory / f'{name}-1-cap{cap}' / 'result.json'
        retries = sorted(directory.glob(f'{name}-retry*-1-cap{cap}/result.json'),
                         key=lambda path: int(path.parent.name.split('-retry', 1)[1].split('-')[0]))
        rows = [loaded_window(path, root) for path in (initial, *retries) if path.is_file()]
        if rows:
            attempts[name] = [{'path': row['path'], 'passed': row['passed'], 'error': row['error'],
                               'measurement': row.get('measurement'), 'link': row.get('link')} for row in rows]
        selected = next((row for row in rows if row['passed']), None)
        if selected:
            windows[name] = selected
    pairs = []
    for order in (schedule[:2], schedule[2:]):
        if not all(windows.get(name, {}).get('passed') for name in order):
            continue
        baseline = next(windows[name]['measurement'] for name in order if name.endswith('-A'))
        candidate = next(windows[name]['measurement'] for name in order if name.endswith('-B'))
        pairs.append({'order': order, **{f'candidate_minus_baseline_{key}': candidate[key] - baseline[key]
                                        for key in ('convergence_ms', 'rss_hwm_kib', 'peak_payload_streams')}})
    expected = (*schedule, 'candidate-fault')
    return {'windows': windows, 'attempts': attempts, 'pairs': pairs,
            'missing_windows': [name for name in expected if name not in attempts],
            'windows_without_passing_attempt': [name for name in expected if name in attempts and name not in windows]}


def direct_block(directory):
    index = json.loads((directory / 'result.json').read_bytes())
    rows = []
    for entry in index['windows']:
        raw = (directory / entry['file']).read_bytes()
        require(sha256(raw).hexdigest() == entry['sha256'], f'{directory}: window hash')
        row = json.loads(raw)
        request, manifest, durable, view = row['events']
        require([event['stage'] for event in row['events']] ==
                ['pull_request', 'manifest_validated', 'current_durable', 'view_published'],
                f'{directory}: incomplete install')
        target = index['manifest']['last']
        require(row['passed'] and manifest['after'] == request['after'] == index['before']['sequence']
                and manifest['last'] == durable['last'] == view['last'] == row['current']['sequence'] == target
                and len(index['files']) == row['verified_files'] and index['keys'] == row['verified_keys'],
                f'{directory}: install does not cover the requested data')
        total = sum(item['bytes'] for item in index['files'].values())
        require(total == row['payload_bytes'], f'{directory}: unexpected payload size')
        out = {'window': row['label'], 'bytes': total, 'result_sha256': entry['sha256']}
        for stage, event in (('durable', durable), ('view', view)):
            elapsed = (event['monotonic_ns'] - request['monotonic_ns']) / 1e9
            out[stage + '_ms'] = elapsed * 1000
            out[stage + '_mib_s'] = total / (1 << 20) / elapsed
        rows.append(out)
    pairs = []
    for i in range(0, len(rows) - 1, 2):
        pair = rows[i:i + 2]
        baseline, candidate = sorted(pair, key=lambda row: row['window'].split('-', 1)[1])
        require(baseline['window'].endswith('-baseline') and candidate['window'].endswith('-candidate'),
                f'{directory}: invalid pair order')
        pairs.append({stage: candidate[stage + '_mib_s'] / baseline[stage + '_mib_s']
                      for stage in ('durable', 'view')})
    return {'passed': index.get('passed', False), 'error': index.get('error'), 'windows': rows,
            'candidate_over_baseline': pairs, 'files': index.get('files'),
            'index_sha256': sha256((directory / 'result.json').read_bytes()).hexdigest()}


def regression_checks(root):
    directory = root / 'gate'
    result = {'mixed_versions': {}}
    for name, field in [('full-regression.log', 'full'), ('reader-race-repeat.log', 'repeated_race')]:
        path = directory / name
        if not path.is_file():
            continue
        top, nested, skipped, failed = defaultdict(int), 0, set(), set()
        rust = 0
        for line in path.read_text().splitlines():
            matched = re.match(r'test result: ok\. (\d+) passed; 0 failed;', line)
            if matched:
                rust += int(matched[1])
            if not line.startswith('{'):
                continue
            event = json.loads(line)
            action, test = event['Action'], event.get('Test')
            if action == 'fail':
                failed.add(event['Package'] + '/' + (test or ''))
            elif test and action == 'skip':
                skipped.add(test)
            elif test and action == 'pass':
                if '/' in test:
                    nested += 1
                else:
                    top[event['Package'] + '/' + test] += 1
        row = {'go_top_level_passes': sum(top.values()), 'go_subtest_passes': nested,
               'skipped_tests': sorted(skipped), 'go_failures': sorted(failed)}
        if field == 'full':
            row['rust_passes'] = rust
        else:
            row['repetitions_by_test'] = dict(sorted(top.items()))
        result[field] = row
    for path in sorted(directory.glob('*source*/*/result.json')):
        raw = json.loads(path.read_bytes())
        result['mixed_versions'][path.parents[1].name] = {
            'passed': raw.get('passed', False), 'network': raw['network'],
            'events': [item['name'] for item in raw['events']],
            'path': path.relative_to(root).as_posix(),
        }
    return result


def evidence_hashes(root):
    directories = {root}
    for name in (*SIDES, 'ordinary', *PHASE_RUNS, 'loaded', 'phase-baseline', 'phase-candidate',
                 'direct-1', 'direct-2', 'footprint', 'gate'):
        parent = root / name
        if parent.is_dir():
            directories.add(parent)
            directories.update(path for path in parent.glob('*-*') if path.is_dir())
    files = {path for directory in directories for path in directory.iterdir() if path.is_file()
             and (path.name in {'result.json', 'build.json', 'alloc.bufio.txt', 'inputs.sha256'}
                  or path.suffix in {'.log', '.jsonl'})}
    files.update(path for path in root.glob('run-*.sh') if path.is_file())
    files.update((root / 'gate').glob('*source*/*/result.json'))
    files.update((root / 'footprint').glob('*.json'))
    hashes = {}
    for path in sorted(files):
        with path.open('rb') as source:
            hashes[path.relative_to(root).as_posix()] = file_digest(source, 'sha256').hexdigest()
    return hashes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    require(root.is_dir(), f'{root}: evidence root does not exist')
    result = {'idle': exact(root), 'ordinary': ordinary(root),
              'phase': {name: phase(root, name) for name in PHASE_RUNS},
              'loaded': loaded(root),
              'direct': {p.name: direct_block(p) for p in sorted(root.glob('direct-[12]'))
                         if (p / 'result.json').is_file()},
              'footprint': {p.stem: json.loads(p.read_bytes()) for p in sorted((root / 'footprint').glob('*.json'))},
              'regression': regression_checks(root), 'evidence_sha256': evidence_hashes(root)}
    args.output.write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')


if __name__ == '__main__':
    main()
