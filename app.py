import sys
import os
import uuid
import time
import sqlite3
from datetime import datetime
from flask import Flask, jsonify, render_template_string

# Fix path so Python finds our project folders
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

app = Flask(__name__)
TASK_LOG = []

# ── TEST IMPORT ON STARTUP ────────────────────────────────────
PIPELINE_AVAILABLE = False
IMPORT_ERROR = ""

try:
    from pipeline import ETLPipeline
    PIPELINE_AVAILABLE = True
    print("✅ Pipeline imported successfully")
except Exception as e:
    IMPORT_ERROR = str(e)
    print(f"❌ Pipeline import failed: {e}")

HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>ETL Pipeline — Yashraj Jagdale</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{
            background:#0d0d0d;color:#00ff88;
            font-family:'Courier New',monospace;
            min-height:100vh;
        }
        header{
            border-bottom:1px solid #00ff88;
            padding:20px 40px;
            display:flex;justify-content:space-between;align-items:center;
        }
        .logo{font-size:1.5em;font-weight:bold}
        .live{display:flex;align-items:center;gap:8px;color:#888}
        .dot{
            width:10px;height:10px;background:#00ff88;
            border-radius:50%;animation:pulse 2s infinite;
        }
        @keyframes pulse{0%,100%{opacity:1}50%{opacity:0.3}}
        .main{padding:40px;max-width:950px;margin:0 auto}
        h1{font-size:3em;margin-bottom:8px}
        .sub{color:#888;margin-bottom:20px}
        .tag{
            display:inline-block;padding:4px 12px;
            border:1px solid;font-size:0.8em;margin:3px;
        }
        .t1{border-color:#3776ab;color:#3776ab}
        .t2{border-color:#e70488;color:#e70488}
        .t3{border-color:#ff9900;color:#ff9900}
        .t4{border-color:#00aaff;color:#00aaff}
        .t5{border-color:#00ff88;color:#00ff88}
        hr{border-color:#1a1a1a;margin:25px 0}
        .stats{
            display:grid;grid-template-columns:repeat(4,1fr);
            gap:15px;margin:25px 0;
        }
        .card{border:1px solid #00ff88;padding:20px;text-align:center}
        .num{font-size:2.5em}
        .lbl{color:#888;font-size:0.75em;margin-top:5px}
        .flow{
            display:flex;justify-content:center;
            align-items:center;gap:10px;flex-wrap:wrap;margin:20px 0;
        }
        .stage{border:1px solid #00ff88;padding:10px 18px;font-size:0.9em}
        .arr{font-size:1.3em}
        .celery-box{
            border:1px solid #ff9900;padding:15px;
            color:#ff9900;font-size:0.85em;margin:20px 0;
        }
        .btns{margin:20px 0;display:flex;flex-wrap:wrap;gap:10px}
        .btn{
            background:transparent;color:#00ff88;
            border:2px solid #00ff88;
            padding:14px 22px;font-size:0.9em;
            cursor:pointer;font-family:monospace;
            transition:all 0.3s;flex:1;min-width:160px;
        }
        .btn:hover{background:#00ff88;color:#000}
        .btn:disabled{opacity:0.4;cursor:not-allowed}
        .btn-c{border-color:#ff9900;color:#ff9900}
        .btn-c:hover{background:#ff9900;color:#000}
        .result{
            margin-top:20px;padding:20px;
            border:1px solid #333;background:#111;
            display:none;white-space:pre-wrap;
            line-height:2;font-size:0.9em;
        }
        .hbox{
            margin-top:20px;border:1px solid #333;
            padding:20px;display:none;
        }
        .hrow{
            display:flex;justify-content:space-between;
            padding:8px 0;border-bottom:1px solid #1a1a1a;font-size:0.85em;
        }
        .warn{
            border:1px solid #ff4444;padding:12px;
            color:#ff4444;margin:15px 0;font-size:0.85em;display:none;
        }
        @media(max-width:600px){
            .stats{grid-template-columns:repeat(2,1fr)}
            h1{font-size:2em}
            .btn{min-width:130px}
        }
    </style>
</head>
<body>
<header>
    <div class="logo">⚡ ETL_Pipeline</div>
    <div class="live"><div class="dot"></div>Live on Render</div>
</header>

<div class="main">
    <h1>⚡ ETL Pipeline</h1>
    <p class="sub">Advanced Data Engineering Project by Yashraj Jagdale</p>

    <div>
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
        <div class="card"><div class="num" id="err-count">0</div><div class="lbl">ERRORS</div></div>
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
        Free tier: in-process simulation with Task IDs.
    </div>

    <div class="btns">
        <button class="btn" id="b1" onclick="runPipeline()">
            ▶ Run Demo Pipeline
        </button>
        <button class="btn btn-c" id="b2" onclick="runCelery()">
            🔄 Queue Celery Task
        </button>
        <button class="btn" id="b3" onclick="getStatus()">
            📊 System Status
        </button>
        <button class="btn" id="b4" onclick="getHistory()">
            📋 Task History
        </button>
    </div>

    <div class="result" id="result"></div>

    <div class="hbox" id="hbox">
        <h3 style="margin-bottom:15px;color:#ff9900">
            📋 Celery Task History
        </h3>
        <div id="hrows"></div>
    </div>
</div>

<script>
// Show result box with message and colour
function show(html, color) {
    var r = document.getElementById('result');
    r.style.display = 'block';
    r.style.color = color || '#00ff88';
    r.innerHTML = html;
}

// Disable/enable all buttons
function setBtns(disabled) {
    ['b1','b2','b3','b4'].forEach(function(id) {
        document.getElementById(id).disabled = disabled;
    });
}

// ── RUN PIPELINE ─────────────────────────────────────────────
async function runPipeline() {
    setBtns(true);
    document.getElementById('b1').textContent = '⏳ Running...';
    show(
        '⏳ Running ETL Pipeline...\n' +
        'Extract → Transform → Validate → Load\n\n' +
        'Please wait 10-30 seconds...',
        '#ffaa00'
    );

    try {
        // timeout after 60 seconds
        var controller = new AbortController();
        var timer = setTimeout(function(){controller.abort()}, 60000);

        var res = await fetch('/run', {signal: controller.signal});
        clearTimeout(timer);
        var d = await res.json();

        if (d.status === 'success') {
            show(
                '✅ Pipeline Complete!\n\n' +
                '━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n' +
                '📥 Rows Extracted   : ' + d.extracted + '\n' +
                '🔧 Rows Transformed : ' + d.transformed + '\n' +
                '💾 Rows Loaded      : ' + d.loaded + '\n' +
                '❌ Rows Failed      : ' + d.failed + '\n' +
                '⏱  Duration         : ' + d.duration + '\n' +
                '━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n' +
                '🗄  Saved to SQLite database\n' +
                '📊 Table: employees',
                '#00ff88'
            );
            document.getElementById('err-count').textContent =
                d.failed > 0 ? d.failed : '0';
        } else {
            show(
                '❌ Pipeline Error\n\n' +
                'Error: ' + d.message + '\n\n' +
                '💡 Tip: Free tier sleeps after inactivity.\n' +
                'Wait 30 seconds and try again.',
                '#ff4444'
            );
        }
    } catch(e) {
        if (e.name === 'AbortError') {
            show(
                '⏱  Request timed out\n\n' +
                'The free tier server takes 50+ seconds\n' +
                'to wake up from sleep. Try again!',
                '#ff4444'
            );
        } else {
            show('❌ Error: ' + e.message, '#ff4444');
        }
    }

    setBtns(false);
    document.getElementById('b1').textContent = '▶ Run Demo Pipeline';
}

// ── CELERY TASK ───────────────────────────────────────────────
async function runCelery() {
    setBtns(true);
    document.getElementById('b2').textContent = '⏳ Queuing...';
    show('🔄 Sending task to Celery queue...\nGenerating Task ID...', '#ff9900');

    try {
        var res = await fetch('/celery/run');
        var d = await res.json();
        show(
            '🔄 Celery Task Queued & Executed!\n\n' +
            '━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n' +
            '📋 Task ID     : ' + d.task_id + '\n' +
            '📌 Task Name   : run_demo_task\n' +
            '⏰ Queued At   : ' + d.queued_at + '\n' +
            '✅ Status      : ' + d.status + '\n' +
            '📊 Result      : ' + d.result + '\n' +
            '━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n' +
            '💡 Production Celery commands:\n' +
            '  celery -A schedulers.pipeline_scheduler worker\n' +
            '  celery -A schedulers.pipeline_scheduler beat\n' +
            '  (Needs Redis server as message broker)',
            '#ff9900'
        );
    } catch(e) {
        show('❌ Error: ' + e.message, '#ff4444');
    }

    setBtns(false);
    document.getElementById('b2').textContent = '🔄 Queue Celery Task';
}

// ── SYSTEM STATUS ─────────────────────────────────────────────
async function getStatus() {
    setBtns(true);
    document.getElementById('b3').textContent = '⏳ Loading...';
    show('⏳ Fetching system status...', '#00aaff');

    try {
        var res = await fetch('/status');
        var d = await res.json();
        var lines = '📊 SYSTEM STATUS\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n';
        for (var k in d) {
            lines += (k + '               ').slice(0,20) + ': ' + d[k] + '\n';
        }
        lines += '━━━━━━━━━━━━━━━━━━━━━━━━━━━━';
        show(lines, '#00aaff');
    } catch(e) {
        show('❌ Error: ' + e.message, '#ff4444');
    }

    setBtns(false);
    document.getElementById('b3').textContent = '📊 System Status';
}

// ── TASK HISTORY ──────────────────────────────────────────────
async function getHistory() {
    setBtns(true);
    document.getElementById('b4').textContent = '⏳ Loading...';

    try {
        var res = await fetch('/celery/history');
        var d = await res.json();
        var hbox = document.getElementById('hbox');
        var hrows = document.getElementById('hrows');
        hbox.style.display = 'block';

        if (!d.tasks || d.tasks.length === 0) {
            hrows.innerHTML =
                '<div style="color:#888;padding:10px">' +
                'No tasks yet.<br>' +
                'Click "Run Demo Pipeline" or<br>' +
                '"Queue Celery Task" first.' +
                '</div>';
        } else {
            hrows.innerHTML = d.tasks.map(function(t) {
                var name = t.task.split('.').pop();
                var time = t.timestamp ? t.timestamp.substring(11,19) : '';
                return '<div class="hrow">' +
                    '<span style="color:#ff9900">' + name + '</span>' +
                    '<span style="color:#888">' + time + '</span>' +
                    '<span style="color:#00ff88">' + t.status + '</span>' +
                    '</div>';
            }).join('');
        }
    } catch(e) {
        show('❌ Error: ' + e.message, '#ff4444');
    }

    setBtns(false);
    document.getElementById('b4').textContent = '📋 Task History';
}
</script>
</body>
</html>
"""

# ── HELPER ────────────────────────────────────────────────────
def add_log(task, status, result=None):
    TASK_LOG.append({
        "task":      task,
        "status":    status,
        "result":    str(result)[:100],
        "timestamp": datetime.now().isoformat()
    })
    if len(TASK_LOG) > 20:
        TASK_LOG.pop(0)


# ── ROUTES ────────────────────────────────────────────────────
@app.route('/')
def home():
    return render_template_string(HTML)


@app.route('/run')
def run_pipeline():
    start = time.time()
    try:
        if not PIPELINE_AVAILABLE:
            return jsonify({
                "status":  "error",
                "message": f"Pipeline import failed: {IMPORT_ERROR}"
            })

        pipeline = ETLPipeline("render_run")
        result   = pipeline.run(source="demo", target_table="employees")
        duration = f"{round(time.time()-start, 2)}s"
        loaded   = result.get("loaded", 0)
        failed   = result.get("failed", 0)

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
    task_id   = str(uuid.uuid4())[:8].upper()
    task_name = "schedulers.pipeline_scheduler.run_demo_task"
    queued_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start     = time.time()

    try:
        if not PIPELINE_AVAILABLE:
            add_log(task_name, "FAILED ❌", IMPORT_ERROR)
            return jsonify({
                "task_id":   task_id,
                "task_name": task_name,
                "queued_at": queued_at,
                "status":    "FAILED ❌",
                "result":    f"Import error: {IMPORT_ERROR}"
            })

        pipeline = ETLPipeline(f"celery_{task_id}")
        result   = pipeline.run(source="demo", target_table="celery_employees")
        duration = round(time.time()-start, 2)
        loaded   = result.get("loaded", 0)

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


@app.route('/debug')
def debug():
    """
    Special route to see exactly what's wrong.
    Visit: https://etl-pipeline-txrp.onrender.com/debug
    """
    import os
    files = []
    for root, dirs, fs in os.walk(BASE_DIR):
        # skip hidden folders
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        for f in fs:
            files.append(os.path.join(root, f).replace(BASE_DIR, ''))

    return jsonify({
        "pipeline_available": PIPELINE_AVAILABLE,
        "import_error":       IMPORT_ERROR,
        "base_dir":           BASE_DIR,
        "python_path":        sys.path[:5],
        "files_found":        files[:30],
        "env_vars":           {
            "PORT":        os.environ.get("PORT", "5000"),
            "PYTHONPATH":  os.environ.get("PYTHONPATH", "not set"),
        }
    })


@app.route('/status')
def status():
    try:
        conn   = sqlite3.connect("etl_pipeline.db")
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        conn.close()
        db_status = f"Connected — {len(tables)} tables"
    except Exception:
        db_status = "SQLite ready"

    return jsonify({
        "status":           "Online ✅",
        "project":          "Advanced ETL Pipeline",
        "author":           "Yashraj Jagdale",
        "version":          "2.0.0",
        "python":           "3.11",
        "database":         db_status,
        "pipeline_ready":   str(PIPELINE_AVAILABLE),
        "celery_mode":      "Task simulation (Redis for production)",
        "tasks_completed":  len(TASK_LOG),
        "live_url":         "https://etl-pipeline-txrp.onrender.com"
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
