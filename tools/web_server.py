import argparse
import json
import os
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import old_algorithm
import our_alogorithm
from compare_runner import build_comparison
from simulation_core import run_many_with_progress, summarize_in_memory

WEB_DIR = os.path.join(os.path.dirname(__file__), 'web')
DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 8000

jobs = {}
jobs_lock = threading.Lock()
job_counter = 0


def _next_job_id() -> str:
    global job_counter
    with jobs_lock:
        job_counter += 1
        return f'job-{job_counter}'


def _set_job(job_id: str, **kwargs):
    with jobs_lock:
        if job_id not in jobs:
            jobs[job_id] = {}
        jobs[job_id].update(kwargs)


def _get_job(job_id: str):
    with jobs_lock:
        return dict(jobs.get(job_id, {}))


def _run_full(job_id: str, num_runs: int, algo: str):
    try:
        total_parts = 2 if algo == 'both' else 1

        def progress_fn(done, total, part_idx):
            pct = ((part_idx + (done / total)) / total_parts)
            _set_job(job_id, progress=pct)

        results = {}

        if algo in ('old', 'both'):
            def cb_old(done, total):
                progress_fn(done, total, 0 if algo == 'both' else 0)
            summary_old = run_many_with_progress(
                num_runs=num_runs,
                strategy=old_algorithm.simulate_car_threshold,
                summarizer=summarize_in_memory,
                config=old_algorithm.DEFAULT_CONFIG.copy(),
                clear_logs=False,
                progress_cb=cb_old,
            )
            results['old'] = summary_old

        if algo in ('our', 'both'):
            def cb_our(done, total):
                part_index = 1 if algo == 'both' else 0
                progress_fn(done, total, part_index)
            summary_our = run_many_with_progress(
                num_runs=num_runs,
                strategy=our_alogorithm.simulate_car_pdf,
                summarizer=summarize_in_memory,
                config=our_alogorithm.DEFAULT_CONFIG.copy(),
                clear_logs=False,
                progress_cb=cb_our,
            )
            results['our'] = summary_our

        compare_sample = build_comparison()
        _set_job(job_id, status='done', progress=1.0, result={'summaries': results, 'sample': compare_sample})
    except Exception as exc:  # pragma: no cover - guardrail
        _set_job(job_id, status='error', error=str(exc))



class CompareHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def _send_json(self, status: int, payload: dict) -> None:
        data = json.dumps(payload).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        parsed = urlparse(self.path)
        length = int(self.headers.get('Content-Length', 0) or 0)
        raw_body = self.rfile.read(length) if length > 0 else b''
        try:
            body = json.loads(raw_body.decode('utf-8')) if raw_body else {}
        except json.JSONDecodeError:
            self._send_json(400, {'error': 'Invalid JSON body'})
            return

        if parsed.path == '/api/compare':
            seed = body.get('seed')
            include_logs = bool(body.get('include_logs', False))
            config_overrides = body.get('config') if isinstance(body.get('config'), dict) else None

            try:
                payload = build_comparison(seed=seed, config_overrides=config_overrides, include_logs=include_logs)
            except Exception as exc:  # pragma: no cover - guardrail for quick diagnostics
                self._send_json(500, {'error': f'Internal error: {exc}'})
                return

            self._send_json(200, payload)
            return

        if parsed.path == '/api/fullrun':
            num_runs = int(body.get('num_runs', 100))
            algo = body.get('algo', 'both')
            if algo not in ('old', 'our', 'both'):
                self._send_json(400, {'error': 'algo must be old|our|both'})
                return
            job_id = _next_job_id()
            _set_job(job_id, status='running', progress=0.0)
            thread = threading.Thread(target=_run_full, args=(job_id, num_runs, algo), daemon=True)
            thread.start()
            self._send_json(200, {'job_id': job_id})
            return

        self.send_error(404, 'Not Found')

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path.startswith('/api/fullrun/'):
            job_id = parsed.path.split('/')[-1]
            job = _get_job(job_id)
            if not job:
                self._send_json(404, {'error': 'job not found'})
                return
            self._send_json(200, job)
            return

        return super().do_GET()


def serve(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT):
    os.makedirs(WEB_DIR, exist_ok=True)
    httpd = ThreadingHTTPServer((host, port), CompareHandler)
    print(f'Serving compare UI on http://{host}:{port}')
    print('Press Ctrl+C to stop')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print('\nShutting down...')
    finally:
        httpd.server_close()


def main():
    parser = argparse.ArgumentParser(description='Run web UI for algorithm comparison')
    parser.add_argument('--host', default=DEFAULT_HOST, help='Host to bind (default 127.0.0.1)')
    parser.add_argument('--port', type=int, default=DEFAULT_PORT, help='Port to bind (default 8000)')
    args = parser.parse_args()
    serve(host=args.host, port=args.port)


if __name__ == '__main__':
    main()
