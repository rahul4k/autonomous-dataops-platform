
"""
Tiny Flask app to browse / view saved logs and parsed classification.
Run: python tools/log_ui.py
Visit: http://127.0.0.1:5000/
"""

import os
import json
from flask import Flask, render_template_string, send_file, abort
from agents.log_reader_agent import parse_log_file

LOG_ROOT = "logs"
app = Flask(__name__)

INDEX_HTML = """
<!doctype html>
<title>ADP Logs UI</title>
<h1>ADP - Saved Logs</h1>
<ul>
{% for job, files in logs.items() %}
  <li><strong>{{job}}</strong>
    <ul>
    {% for f in files %}
      <li><a href="/view?path={{f}}">{{f.split('/')[-1]}}</a></li>
    {% endfor %}
    </ul>
  </li>
{% endfor %}
</ul>
"""

VIEW_HTML = """
<!doctype html>
<title>View Log</title>
<h2>{{filename}}</h2>
<h3>Parsed Summary</h3>
<pre>{{summary}}</pre>
{% if rootcause %}
<h3>Root Cause (detected)</h3>
<pre>{{rootcause}}</pre>
{% endif %}
<h3>Log excerpt (first 4000 chars)</h3>
<pre style="white-space: pre-wrap; background:#f5f5f5; padding:8px;">{{excerpt}}</pre>
<p><a href="/">Back</a></p>
"""

def list_logs():
    out = {}
    if not os.path.exists(LOG_ROOT):
        return out
    for job in sorted(os.listdir(LOG_ROOT)):
        job_dir = os.path.join(LOG_ROOT, job)
        if os.path.isdir(job_dir):
            files = sorted([os.path.join(job_dir, f) for f in os.listdir(job_dir) if f.endswith(".log")])
            out[job] = files
    return out

@app.route("/")
def index():
    logs = list_logs()
    return render_template_string(INDEX_HTML, logs=logs)

@app.route("/view")
def view():
    from flask import request
    path = request.args.get("path")
    if not path or not os.path.exists(path):
        abort(404)
    parsed = parse_log_file(path)
    rootcause_path = path + ".root_cause.json"
    rootcause = None
    if os.path.exists(rootcause_path):
        try:
            with open(rootcause_path) as fh:
                rootcause = json.dumps(json.load(fh), indent=2)
        except Exception:
            rootcause = "Could not read root cause JSON"
    return render_template_string(VIEW_HTML,
                                  filename=os.path.basename(path),
                                  summary=json.dumps(parsed["summary"], indent=2),
                                  excerpt=parsed["sample_excerpt"][:4000],
                                  rootcause=rootcause)

if __name__ == "__main__":
    # Install instructions: pip install flask
    app.run(debug=True, port=5000)
