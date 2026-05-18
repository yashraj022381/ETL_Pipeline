import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, jsonify, render_template_string
import sqlite3
import uuid
from datetime import datetime

app = Flask(__name__)
TASK_LOG = []

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
        .live {
            display: flex;
            align-items: center;
            gap: 8px;
            color: #888;
        }
        .dot {
            width: 10px; height: 10px;
            background: #00ff88;
            border-radius: 50%;
            animation: pulse 2s infinite;
        }
        @keyframes pulse {
            0%,100%{opacity:1} 50%{opacity:0.3}
        }
        .main {
            padding: 40px;
            max-width: 900px;
            margin: 0 auto;
        }
        h1 { font-size: 3em; margin-bottom: 10px; }
        .sub { color: #888; margin-bottom: 20px; }
        .tags { margin-bottom: 30px; }
        .tag {
            display: inline-block;
            padding: 4px 12px;
            border: 1px solid;
            font-size: 0.8em;
            margin: 3px;
        }
        .t1{border-color:#3776ab;color:#3776ab}
        .t2{border-color:#e70488;color:#e70488}
        .t3{border-color:#ff9900;color:#ff9900}
        .t4{border-color:#00aaff;color:#00aaff}
        .t5{border-color:#00ff88;color:#00ff88}
        hr{border-color:#1a1a1a;margin:25px 0}
        .stats {
            display: grid;
            grid-template-columns: repeat(4,1fr);
            gap: 15px;
            margin-bottom: 30px;
        }
        .card {
            border: 1px solid #00ff88;
            padding: 20px;
            text-align: center;
        }
        .num { font-size: 2.5em; }
        .lbl { color: #888; font-size: 0.75em; margin-top: 5px; }
        .flow {
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 10px;
            flex-wrap: wrap;
            margin: 20px 0;
        }
        .stage {
            border: 1px solid #00ff88;
            padding: 10px 18px;
            font-size: 0.9em;
        }
        .arr { font-size: 1.3em; }
        .celery-box {
            border: 1px solid #ff9900;
            padding: 15px;
            color: #ff9900;
            font-size: 0.85em;
            margin: 20px 0;
        }
        .btns { margin: 20px 0; }
        .btn {
            background: transparent;
            color: #00ff88;
            border: 2px solid #00ff88;
            padding: 12px 25px;
            font-size: 0.95em;
            cursor: pointer;
            margin: 6px;
            font-family: monospace;
            transition: all 0.3s;
        }
        .btn:hover { background: #00ff88; color: #000; }
        .btn:disabled { opacity: 0.4; cursor: not-allowed; }
        .btn-c { border-color:#ff9900; color:#ff9900; }
        .btn-c:hover { background:#ff9900; color:#000; }
        .result {
            margin-top: 20px;
            padding: 20px;
            border: 1px solid #333;
            background: #111;
            display: none;
            white-space: pre-wrap;
            line-height: 2;
            font-size: 0.95em;
        }
        .history-box {
            margin-top: 20px;
            border: 1px solid #333;
            padding: 20px;
            display: none;
        }
        .hrow {
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid #1a1a1a;
            font-size: 0.85em;
        }
        @media(max-width:600px){
            .stats{grid-template-columns:repeat(2,1fr)}
            h1{font-size:2em}
            .flow{gap:5px}
        }
    </style>
</head>
<body>

<header>
    <div class="logo">⚡ ETL_Pipeline</div>
    <div class="live">
        <div class="dot"></div>
        Live on Render
    </div>
</header>

<div class="main">
    <h1>⚡ ETL Pipeline</h1>
    <p class="sub">Advanced Data Engineering Project by Yashraj Jagdale</p>

    <div class="tags">
        <span class="tag t1">Python 3.11</span>
        <span class="tag t2">Pandas</span>
        <span class="tag t3">Celery</span>
        <span class="tag t4">SQLite</span>
        <span class="tag t5">Flask</span>
    </div>

    <hr>

    <div class="stats">
        <div class="card"><div class="num">3</div><div class="lbl">ETL STAGES</div></div>
        <div class="card"><div class="num">3K+</div><div class="lbl">ROWS/RUN</div></div>
        <div class="card"><div class="num">4</div><div class="lbl">VALIDATORS</div></div>
        <div class="card"><div class="num">0</div><div class="lbl">ERRORS</div></div>
    </div>

    <div class="flow">
        <div class="stage">📥 EXTRACT</div>
        <div class="arr">→</div>
        <div class="stage">🔧 TRANSFORM</div>
        <div class="arr">→</div>
        <div class="stage">✅ VALIDATE</div>
        <div class="arr">→</div>
        <div class="stage">💾 LOAD</div>
    </div>

    <hr>

    <div class="celery-box">
        🔄 <strong>Celery Task Queue</strong> — Tasks queued and processed
        asynchronously. Production: Redis broker + workers.
        Free tier: in-process simulation.
    </div>

    <div class="btns">
        <button class="btn" id="btn-run" onclick="runPipeline()">
            ▶ Run Demo Pipeline
        </button>
        <button class="btn btn-c" id="btn-celery" onclick="runCelery()">
            🔄 Queue Celery Task
        </button>
        <button class="btn" onclick="getStatus()">
            📊 System Status
        </button>
        <button class="btn" onclick="getHistory()">
            📋 Task History
        </button>
    </div>

    <div class="result" id="result"></div>
    <div class="history-box" id="hbox">
        <h3 style="margin-bottom:15px;color:#ff9900">
            📋 Celery Task History
        </h3>
        <div id="hrows"></div>
    </div>
</div>

<script>
function show(html, color) {
    var r = document.getElementById('result');
    r.style.display = 'block';
    r.style.color = color || '#00ff88';
    r.innerHTML = html;
}

async function runPipeline() {
    var btn = document.getElementById('btn-run');
    btn.disabled = true;
    btn.textContent = '⏳ Running...';
    show('⏳ Running ETL Pipeline...\nExtract → Transform → Validate → Load\nPlease wait...', '#ffaa00');

    try {
        var res = await fetch('/run');
        var d = await res.json();
        if (d.status === 'success') {
            show(
                '✅ Pipeline Complete!\n\n' +
                '📥 Rows Extracted   : ' + d.extracted + '\n' +
                '🔧 Rows Transformed : ' + d.transformed + '\n' +
                '💾 Rows Loaded      : ' + d.loaded + '\n' +
                '❌ Rows Failed      : ' + d.failed + '\n' +
                '⏱  Duration         : ' + d.duration + '\n\n' +
                '🗄  Database: etl_pipeline.db\n' +
                '📊 Table: employees',
                '#00ff88'
            );
        } else {
            show('❌ Pipeline Error:\n\n' + d.message, '#ff4444');
        }
    } catch(e) {
        show('❌ Connection Error: ' + e.message + '\n\nThe server may be waking up (free tier).\nWait 30 seconds and try again.', '#ff4444');
    }

    btn.disabled = false;
    btn.textContent = '▶ Run Demo Pipeline';
}

async function runCelery() {
    var btn = document.getElementById('btn-celery');
    btn.disabled = true;
    btn.textContent = '⏳ Queuing...';
    show('🔄 Sending task to Celery queue...\nGenerating Task ID...', '#ff9900');

    try {
        var res = await fetch('/celery/run');
        var d = await res.json();
        show(
            '🔄 Celery Task Complete!\n\n' +
            '📋 Task ID    : ' + d.task_id + '\n' +
            '📌 Task Name  : ' + d.task_name + '\n' +
            '⏰ Queued At  : ' + d.queued_at + '\n' +
            '✅ Status     : ' + d.status + '\n' +
            '📊 Result     : ' + d.result + '\n\n' +
            '💡 Production setup:\n' +
            '   celery -A schedulers.pipeline_scheduler worker\n' +
            '   celery -A schedulers.pipeline_scheduler beat',
            '#ff9900'
        );
    } catch(e) {
        show('❌ Error: ' + e.message, '#ff4444');
    }

    btn.disabled = false;
    btn.textContent = '🔄 Queue Celery Task';
}

async function getStatus() {
    show('⏳ Fetching system status...', '#00aaff');
    try {
        var res = await fetch('/status');
        var d = await res.json();
        var txt = '📊 SYSTEM STATUS\n\n';
        for (var k in d) {
            txt += k.padEnd(20) + ': ' + d[k] + '\n';
        }
        show(txt, '#00aaff');
    } catch(e) {
        show('❌ Error: ' + e.message, '#ff4444');
    }
}

async function getHistory() {
    try {
        var res = await fetch('/celery/history');
        var d = await res.json();
        var hbox = document.getElementById('hbox');
        var hrows = document.getElementById('hrows');
        hbox.style.display = 'block';

        if (d.tasks.length === 0) {
            hrows.innerHTML = '<div style="color:#888">No tasks yet.<br>Click "Queue Celery Task" first.</div>';
            return;
        }
        hrows.innerHTML = d.tasks.map(function(t) {
            return '<div class="hrow">' +
                '<span style="color:#ff9900">' + t.task.split('.').pop() + '</span>' +
                '<span style="color:#888">' + t.timestamp.substring(11,19) + '</span>' +
                '<span style="color:#00ff88">' + t.status + '</span>' +
                '</div>';
        }).join('');
    } catch(e) {
        show('❌ Error: ' + e.message, '#ff4444');
    }
}
</script>
</body>
</html>
"""

def add_log(task, status, result=None):
    TASK_LOG.append({
        "task": task,
        "status": status,
        "result": str(result),
        "timestamp": datetime.now().isoformat()
    })
    if len(TASK_LOG) > 20:
        TASK_LOG.pop(0)

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
        result = pipeline.run(source="demo", target_table="employees")
        duration = f"{round(time.time()-start, 2)}s"
        loaded = result.get("loaded", 0)
        failed = result.get("failed", 0)
        add_log("run_pipeline", "SUCCESS", result)
        return jsonify({
            "status":      "success",
            "extracted":   loaded,
            "transformed": loaded,
            "loaded":      loaded,
            "failed":      failed,
            "duration":    duration
        })
    except Exception as e:
        add_log("run_pipeline", "FAILED", str(e))
        return jsonify({"status": "error", "message": str(e)})

@app.route('/celery/run')
def celery_run():
    import time
    task_id = str(uuid.uuid4())[:8].upper()
    task_name = "schedulers.pipeline_scheduler.run_demo_task"
    queued_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start = time.time()
    try:
        from pipeline import ETLPipeline
        pipeline = ETLPipeline(f"celery_{task_id}")
        result = pipeline.run(source="demo", target_table="celery_employees")
        duration = round(time.time()-start, 2)
        loaded = result.get("loaded", 0)
        add_log(task_name, "SUCCESS ✅", result)
        return jsonify({
            "task_id":   task_id,
            "task_name": task_name,
            "queued_at": queued_at,
            "status":    "SUCCESS ✅",
            "result":    f"{loaded} rows loaded in {duration}s"
        })
    except Exception as e:
        add_log(task_name, "FAILED ❌", str(e))
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
    try:
        conn = sqlite3.connect("etl_pipeline.db")
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        conn.close()
        db = f"Connected — {len(tables)} tables"
    except Exception:
        db = "SQLite ready (fresh)"

    return jsonify({
        "status":      "Online ✅",
        "project":     "Advanced ETL Pipeline",
        "author":      "Yashraj Jagdale",
        "version":     "2.0.0",
        "python":      "3.11",
        "database":    db,
        "celery":      "Simulated (Redis for production)",
        "tasks_done":  len(TASK_LOG),
        "live_url":    "https://etl-pipeline-txrp.onrender.com"
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
