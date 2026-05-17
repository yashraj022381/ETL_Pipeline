import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, jsonify, render_template_string
import sqlite3
import json
from datetime import datetime

app = Flask(__name__)

# ── CELERY SETUP ─────────────────────────────────────────────
# On Render free tier, we simulate Celery behaviour
# Real Celery needs Redis server running separately
# We use a task queue pattern without external Redis

TASK_LOG = []   # in-memory task history (like a simple Redis)

def add_task_to_log(task_name, status, result=None):
    """Simulate Celery task tracking"""
    TASK_LOG.append({
        "task": task_name,
        "status": status,
        "result": result,
        "timestamp": datetime.now().isoformat()
    })
    # Keep only last 10 tasks
    if len(TASK_LOG) > 10:
        TASK_LOG.pop(0)

# ── HTML DASHBOARD ────────────────────────────────────────────
HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>ETL Pipeline — Yashraj Jagdale</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { margin:0; padding:0; box-sizing:border-box; }
        body {
            background: #0d0d0d;
            color: #00ff88;
            font-family: 'Courier New', monospace;
            min-height: 100vh;
        }
        header {
            border-bottom: 1px solid #00ff88;
            padding: 20px 40px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .logo { font-size: 1.5em; font-weight: bold; }
        .status-dot {
            width: 10px; height: 10px;
            background: #00ff88;
            border-radius: 50%;
            display: inline-block;
            margin-right: 8px;
            animation: pulse 2s infinite;
        }
        @keyframes pulse {
            0%,100% { opacity:1; }
            50% { opacity:0.3; }
        }
        .main { padding: 40px; max-width: 900px; margin: 0 auto; }
        h1 { font-size: 3em; margin-bottom: 10px; }
        .sub { color: #888; margin-bottom: 40px; font-size: 1.1em; }

        .stats {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 20px;
            margin-bottom: 40px;
        }
        .stat-card {
            border: 1px solid #00ff88;
            padding: 20px;
            text-align: center;
        }
        .stat-num { font-size: 2.5em; color: #00ff88; }
        .stat-label { color: #888; font-size: 0.8em; margin-top: 5px; }

        .pipeline-flow {
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 10px;
            margin: 30px 0;
            flex-wrap: wrap;
        }
        .stage {
            border: 1px solid #00ff88;
            padding: 10px 20px;
            font-size: 0.9em;
        }
        .arrow { color: #00ff88; font-size: 1.5em; }

        .controls { margin: 30px 0; }
        .btn {
            background: transparent;
            color: #00ff88;
            border: 2px solid #00ff88;
            padding: 15px 30px;
            font-size: 1em;
            cursor: pointer;
            margin: 8px;
            font-family: monospace;
            transition: all 0.3s;
            min-width: 150px;
        }
        .btn:hover { background: #00ff88; color: #000; }
        .btn:disabled { opacity: 0.5; cursor: not-allowed; }
        .btn-celery {
            border-color: #ff9900;
            color: #ff9900;
        }
        .btn-celery:hover { background: #ff9900; color: #000; }

        .result {
            margin-top: 20px;
            padding: 20px;
            border: 1px solid #333;
            background: #111;
            display: none;
            white-space: pre-wrap;
            line-height: 1.8;
        }
        .task-history {
            margin-top: 30px;
            border: 1px solid #333;
            padding: 20px;
        }
        .task-row {
            padding: 8px 0;
            border-bottom: 1px solid #1a1a1a;
            display: flex;
            justify-content: space-between;
            font-size: 0.85em;
        }
        .tag {
            display: inline-block;
            padding: 3px 10px;
            border: 1px solid;
            font-size: 0.75em;
            margin: 3px;
        }
        .tag-python { border-color: #3776ab; color: #3776ab; }
        .tag-pandas { border-color: #e70488; color: #e70488; }
        .tag-celery { border-color: #ff9900; color: #ff9900; }
        .tag-sqlite { border-color: #00aaff; color: #00aaff; }
        .tag-flask  { border-color: #00ff88; color: #00ff88; }
        hr { border-color: #1a1a1a; margin: 30px 0; }
        .celery-info {
            border: 1px solid #ff9900;
            padding: 15px;
            margin: 20px 0;
            color: #ff9900;
            font-size: 0.9em;
        }
    </style>
</head>
<body>

<header>
    <div class="logo">⚡ ETL_Pipeline</div>
    <div>
        <span class="status-dot"></span>
        <span style="color:#888; font-size:0.9em">Live on Render</span>
    </div>
</header>

<div class="main">
    <h1>⚡ ETL Pipeline</h1>
    <p class="sub">Advanced Data Engineering Project by Yashraj Jagdale</p>

    <div>
        <span class="tag tag-python">Python 3.11</span>
        <span class="tag tag-pandas">Pandas</span>
        <span class="tag tag-celery">Celery</span>
        <span class="tag tag-sqlite">SQLite</span>
        <span class="tag tag-flask">Flask</span>
    </div>

    <hr>

    <div class="stats">
        <div class="stat-card">
            <div class="stat-num">3</div>
            <div class="stat-label">ETL STAGES</div>
        </div>
        <div class="stat-card">
            <div class="stat-num">3K+</div>
            <div class="stat-label">ROWS/RUN</div>
        </div>
        <div class="stat-card">
            <div class="stat-num">4</div>
            <div class="stat-label">VALIDATORS</div>
        </div>
        <div class="stat-card">
            <div class="stat-num">0</div>
            <div class="stat-label">ERRORS</div>
        </div>
    </div>

    <div class="pipeline-flow">
        <div class="stage">📥 EXTRACT</div>
        <div class="arrow">→</div>
        <div class="stage">🔧 TRANSFORM</div>
        <div class="arrow">→</div>
        <div class="stage">✅ VALIDATE</div>
        <div class="arrow">→</div>
        <div class="stage">💾 LOAD</div>
    </div>

    <hr>

    <div class="celery-info">
        🔄 <strong>Celery Task Queue</strong> — Tasks are queued and processed
        asynchronously. In production: Redis broker + multiple workers.
        On free tier: in-process task simulation.
    </div>

    <div class="controls">
        <button class="btn" onclick="runPipeline('demo')">
            ▶ Run Demo Pipeline
        </button>
        <button class="btn btn-celery" onclick="runCeleryTask()">
            🔄 Queue Celery Task
        </button>
        <button class="btn" onclick="getStatus()">
            📊 System Status
        </button>
        <button class="btn" onclick="getTaskHistory()">
            📋 Task History
        </button>
    </div>

    <div class="result" id="result"></div>

    <div class="task-history" id="history" style="display:none">
        <h3 style="margin-bottom:15px">📋 Celery Task History</h3>
        <div id="history-rows"></div>
    </div>

</div>

<script>
async function runPipeline(source) {
    const r = document.getElementById('result');
    r.style.display = 'block';
    r.style.color = '#ffaa00';
    r.innerHTML = '⏳ Running ETL Pipeline...\nExtract → Transform → Validate → Load';

    try {
        const res = await fetch('/run?source=' + source);
        const d = await res.json();
        if (d.status === 'success') {
            r.style.color = '#00ff88';
            r.innerHTML =
                '✅ Pipeline Complete!\n\n' +
                '📥 Rows Extracted  : ' + d.extracted  + '\n' +
                '🔧 Rows Transformed: ' + d.transformed + '\n' +
                '💾 Rows Loaded     : ' + d.loaded     + '\n' +
                '❌ Rows Failed     : ' + d.failed     + '\n' +
                '⏱  Duration        : ' + d.duration   + '\n\n' +
                '🗄  Saved to: etl_pipeline.db → employees table';
        } else {
            r.style.color = '#ff4444';
            r.innerHTML = '❌ Error: ' + d.message;
        }
    } catch(e) {
        r.style.color = '#ff4444';
        r.innerHTML = '❌ ' + e;
    }
}

async function runCeleryTask() {
    const r = document.getElementById('result');
    r.style.display = 'block';
    r.style.color = '#ff9900';
    r.innerHTML = '🔄 Sending task to Celery queue...\nTask ID being generated...';

    try {
        const res = await fetch('/celery/run');
        const d = await res.json();
        r.style.color = '#ff9900';
        r.innerHTML =
            '🔄 Celery Task Queued!\n\n' +
            '📋 Task ID    : ' + d.task_id    + '\n' +
            '📌 Task Name  : ' + d.task_name  + '\n' +
            '⏰ Queued At  : ' + d.queued_at  + '\n' +
            '✅ Status     : ' + d.status     + '\n\n' +
            '💡 Result     : ' + d.result     + '\n\n' +
            'In production: Redis broker routes this to a\n' +
            'Celery worker process running separately.';
    } catch(e) {
        r.style.color = '#ff4444';
        r.innerHTML = '❌ ' + e;
    }
}

async function getStatus() {
    const r = document.getElementById('result');
    r.style.display = 'block';
    r.style.color = '#00aaff';
    const res = await fetch('/status');
    const d = await res.json();
    r.innerHTML = Object.entries(d)
        .map(([k,v]) => k.padEnd(20) + ': ' + v)
        .join('\n');
}

async function getTaskHistory() {
    const res = await fetch('/celery/history');
    const d = await res.json();
    const h = document.getElementById('history');
    const rows = document.getElementById('history-rows');
    h.style.display = 'block';

    if (d.tasks.length === 0) {
        rows.innerHTML = '<div style="color:#888">No tasks yet. Click "Queue Celery Task" first.</div>';
        return;
    }

    rows.innerHTML = d.tasks.map(t =>
        '<div class="task-row">' +
        '<span style="color:#ff9900">' + t.task + '</span>' +
        '<span style="color:#888">' + t.timestamp + '</span>' +
        '<span style="color:#00ff88">' + t.status + '</span>' +
        '</div>'
    ).join('');
}
</script>
</body>
</html>
"""

# ── ROUTES ────────────────────────────────────────────────────

@app.route('/')
def home():
    return render_template_string(HTML)


@app.route('/run')
def run_pipeline():
    import time
    start = time.time()
    try:
        from pipeline import ETLPipeline
        pipeline = ETLPipeline("render_run")
        result = pipeline.run(
            source="demo",
            target_table="employees"
        )
        duration = f"{round(time.time()-start, 2)}s"
        loaded = result.get("loaded", 0)
        failed = result.get("failed", 0)

        add_task_to_log("run_pipeline", "SUCCESS", {
            "loaded": loaded, "failed": failed
        })

        return jsonify({
            "status":      "success",
            "extracted":   loaded,
            "transformed": loaded,
            "loaded":      loaded,
            "failed":      failed,
            "duration":    duration
        })
    except Exception as e:
        add_task_to_log("run_pipeline", "FAILED", str(e))
        return jsonify({"status": "error", "message": str(e)})


@app.route('/celery/run')
def celery_run():
    """
    Simulates Celery task queueing.

    In production with Redis:
        from schedulers.pipeline_scheduler import run_demo_task
        task = run_demo_task.delay()
        return jsonify({"task_id": task.id, "status": "PENDING"})

    On free tier without Redis, we run synchronously
    but show the Celery task pattern.
    """
    import uuid
    import time

    task_id = str(uuid.uuid4())[:8].upper()
    task_name = "schedulers.pipeline_scheduler.run_demo_task"
    queued_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Run the actual pipeline (simulating worker execution)
    try:
        start = time.time()
        from pipeline import ETLPipeline
        pipeline = ETLPipeline(f"celery_{task_id}")
        result = pipeline.run(source="demo", target_table="celery_employees")
        duration = round(time.time()-start, 2)

        add_task_to_log(task_name, "SUCCESS ✅", result)

        return jsonify({
            "task_id":   task_id,
            "task_name": task_name,
            "queued_at": queued_at,
            "status":    "SUCCESS ✅",
            "result":    f"{result.get('loaded', 0)} rows loaded in {duration}s"
        })
    except Exception as e:
        add_task_to_log(task_name, "FAILED ❌", str(e))
        return jsonify({
            "task_id":   task_id,
            "task_name": task_name,
            "queued_at": queued_at,
            "status":    "FAILED ❌",
            "result":    str(e)
        })


@app.route('/celery/history')
def celery_history():
    return jsonify({"tasks": list(reversed(TASK_LOG))})


@app.route('/status')
def status():
    # Check database
    try:
        conn = sqlite3.connect("etl_pipeline.db")
        cursor = conn.cursor()
        tables = cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        conn.close()
        db_status = f"✅ Connected ({len(tables)} tables)"
    except Exception:
        db_status = "⚠️  SQLite (fresh)"

    return jsonify({
        "status":        "✅ Online",
        "project":       "Advanced ETL Pipeline",
        "author":        "Yashraj Jagdale",
        "version":       "2.0.0",
        "python":        "3.11.9",
        "database":      db_status,
        "celery_mode":   "Simulated (Redis needed for production)",
        "tasks_run":     len(TASK_LOG),
        "endpoints":     "/  |  /run  |  /celery/run  |  /celery/history  |  /status"
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
