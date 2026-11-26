"""Bracket Windows QPC with WSL CLOCK_MONOTONIC before and after a capture."""
import argparse
from contextlib import ExitStack, contextmanager
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import time


def respond():
    for line in sys.stdin.buffer:
        before = time.perf_counter_ns()
        utc = time.time_ns()
        after = time.perf_counter_ns()
        print(int(line), before, utc, after, flush=True)


def windows_path(path):
    return subprocess.check_output(['wslpath', '-w', str(Path(path).resolve())], text=True).strip()


def probe(helper, first):
    rows = []
    for token in range(first, first + 12):
        start = time.monotonic_ns()
        helper.stdin.write(f'{token}\n'.encode('ascii'))
        helper.stdin.flush()
        if not select.select([helper.stdout], [], [], 10)[0]:
            raise TimeoutError('Windows clock probe timed out')
        nonce, before, utc, after = map(int, helper.stdout.readline().split())
        end = time.monotonic_ns()
        if nonce != token or before > after:
            raise ValueError('invalid Windows clock reply')
        rows.append(dict(token=token, guest_send_ns=start, guest_recv_ns=end,
                         host_before_ns=before, host_after_ns=after, host_utc_ns=utc))
    return rows


def offset_bounds(rows):
    """Causal bounds on host monotonic minus guest monotonic during one burst."""
    lo = max(r['host_after_ns'] - r['guest_recv_ns'] for r in rows)
    hi = min(r['host_before_ns'] - r['guest_send_ns'] for r in rows)
    if lo > hi:
        raise ValueError('clock replies have no common offset within this burst')
    return lo, hi


def clock_report(pre, post):
    return {'schema': 1, 'offset': 'Windows perf_counter_ns minus WSL CLOCK_MONOTONIC ns',
            'pre': pre, 'post': post, 'pre_bounds_ns': offset_bounds(pre),
            'post_bounds_ns': offset_bounds(post)}


def close_helper(process):
    try:
        process.stdin.close()
        if process.wait(timeout=5):
            raise RuntimeError('Windows clock helper failed')
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        process.stdout.close()


def stop_sampler(process, stop):
    stop.touch()
    try:
        if process.wait(timeout=15):
            raise RuntimeError('Windows disk sampler failed; see sampler.log')
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


@contextmanager
def HostWindow(windows_python, directory, sampler_script, drive='F', interval_ms=40):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    disk = directory / 'disk'
    stop = directory / 'stop'
    with ExitStack() as stack:
        log = stack.enter_context((directory / 'sampler.log').open('wb'))
        helper = subprocess.Popen([str(windows_python), '-I', '-u', windows_path(__file__), '--respond'],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log)
        stack.callback(close_helper, helper)
        pre = probe(helper, 0)
        offset_bounds(pre)
        sampler = subprocess.Popen([str(windows_python), '-E', '-s', '-B', windows_path(sampler_script), '--drive', drive,
                                    '--output-dir', windows_path(disk), '--interval-ms', str(interval_ms),
                                    '--duration-sec', '150', '--stop-file', windows_path(stop)],
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        stack.callback(stop_sampler, sampler, stop)
        deadline = time.monotonic() + 25
        while not (disk / 'ready.json').exists():
            if sampler.poll() is not None or time.monotonic() >= deadline:
                raise RuntimeError(f'Windows disk sampler did not become ready in {directory}')
            time.sleep(.025)
        try:
            yield
        finally:
            post = probe(helper, len(pre))
            report = clock_report(pre, post)
            (directory / 'clocks.json').write_text(json.dumps(report, indent=2) + '\n')


def run_command(windows_python, output, command):
    if output.exists() or output.is_symlink():
        raise FileExistsError(output)
    partial = output.with_name(output.name + '.partial')
    stream = partial.open('x', encoding='utf-8')
    try:
        with stream:
            if output.exists() or output.is_symlink():
                raise FileExistsError(output)
            status, report = bracket_command(windows_python, command)
            if status:
                return status
            stream.write(json.dumps(report, indent=2) + '\n')
        os.link(partial, output)
        return 0
    finally:
        partial.unlink()


def bracket_command(windows_python, command):
    with subprocess.Popen([str(windows_python), '-I', '-u', windows_path(__file__), '--respond'],
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE) as helper:
        report = None
        try:
            pre = probe(helper, 0)
            offset_bounds(pre)
            result = subprocess.run(command)
            if result.returncode:
                status = result.returncode if result.returncode > 0 else 128 - result.returncode
                return status, None
            report = clock_report(pre, probe(helper, len(pre)))
        finally:
            try:
                close_helper(helper)
            except (OSError, RuntimeError, subprocess.SubprocessError):
                if report is not None:
                    raise
    return 0, report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--respond', action='store_true')
    mode.add_argument('--windows-python', type=Path, help='WSL path to Windows python.exe')
    parser.add_argument('--output', type=Path, help='new clock receipt file')
    parser.add_argument('command', nargs=argparse.REMAINDER, help='command and its arguments after --')
    args = parser.parse_args()
    if args.respond:
        respond()
        return 0
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if args.output is None or not command:
        parser.error('--windows-python requires --output and a command after --')
    try:
        return run_command(args.windows_python, args.output, command)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f'{parser.prog}: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
