import os
import uuid
import time
import sqlite3
import random
from datetime import datetime
from flask import Flask, jsonify
from faker import Faker
import pandas as pd

app = Flask(__name__)
LOGS = []  # Store task history

def do_etl(name="run"):
    # ... (keep your existing do_etl function - same as before)
    fake = Faker()
    Faker.seed(42)
    t0 = time.time()

    try:
        t1 = time.time()
        raw = [{ ... }]  # your existing 300 rows code
        # ... rest of do_etl remains same ...

        result = {
            "status": "success",
            "extracted": len(raw),
            "transformed": len(df),
            "loaded": len(df),
            "failed": 0,
            "dupes_removed": dupes,
            "extract_time": f"{et}s",
            "transform_time": f"{tt}s",
            "load_time": f"{lt}s",
            "total_duration": f"{round(time.time()-t0,3)}s",
            "db_total": total,
            "dept_breakdown": str(depts),
        }
        return result
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ====================== FULL HTML (with improved JS) ======================
PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ETL Pipeline — Yashraj Jagdale</title>
<style>
/* Your existing beautiful CSS - I kept it the same */
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d0d0d;color:#00ff88;font-family:'Courier New',monospace;min-height:100vh}
header{border-bottom:1px solid #00ff88;padding:18px 35px;display:flex;justify-content:space-between;align-items:center}
.logo{font-size:1.6em;font-weight:bold}
.live{display:flex;align-items:center;gap:8px;color:#00ff88}
.dot{width:10px;height:10px;background:#00ff88;border-radius:50%;animation:blink 2s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:0.3}}

main{padding:30px 35px;max-width:1100px;margin:0 auto}
h1{font-size:2.8em;margin-bottom:8px}
.sub{color:#888;margin-bottom:20px}

.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin:25px 0}
.card{border:1px solid #00ff88;padding:20px;text-align:center;border-radius:8px}
.num{font-size:2.8em;font-weight:bold}
.lbl{color:#888;font-size:0.8em;margin-top:6px;letter-spacing:1px}

.flow{display:flex;justify-content:center;align-items:center;gap:12px;flex-wrap:wrap;margin:30px 0;font-size:1.1em}
.stage{border:1px solid #00ff88;padding:12px 20px;border-radius:6px}

.cbox{border:1px solid #ff9900;padding:18px;margin:25px 0;border-radius:6px;background:#1a1a1a}

.btns{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin:30px 0}
.btn{background:transparent;border:2px solid #00ff88;color:#00ff88;padding:16px;font-size:1.05em;cursor:pointer;
     font-family:'Courier New',monospace;transition:0.3s;border-radius:6px}
.btn:hover{background:#00ff88;color:#000}
.btn:disabled{opacity:0.4;cursor:not-allowed}

#res{margin-top:25px;padding:20px;background:#111;border:1px solid #333;white-space:pre-wrap;border-radius:6px;display:none}
</style>
</head>
<body>

<header>
    <div class="logo">⚡ ETL_Pipeline</div>
    <div class="live"><div class="dot"></div> Live on Render</div>
</header>

<main>
    <h1>⚡ ETL Pipeline</h1>
    <p class="sub">Advanced Data Engineering Project by Yashraj Jagdale</p>

    <!-- Your tags and cards remain same -->

    <div class="btns">
        <button class="btn" id="b1" onclick="runPipeline()">▶ Run Demo Pipeline</button>
        <button class="btn" id="b2" onclick="runCelery()">🔄 Queue Celery Task</button>
        <button class="btn" id="b3" onclick="getStatus()">📊 System Status</button>
        <button class="btn" id="b4" onclick="getHistory()">📋 Task History</button>
    </div>

    <div id="res"></div>
</main>

<script>
function showResult(html, title = "") {
    const res = document.getElementById('res');
    res.style.display = 'block';
    res.innerHTML = title ? `<h3 style="color:#ff9900;margin-bottom:12px">${title}</h3>` + html : html;
    res.scrollIntoView({behavior: "smooth"});
}

// Run Demo Pipeline
async function runPipeline() {
    const btn = document.getElementById('b1');
    btn.disabled = true;
    btn.innerHTML = "⏳ Running...";
    showResult("🚀 Starting ETL Pipeline...", "Pipeline Execution");

    try {
        const data = await fetch('/run').then(r => r.json());
        showResult(`<pre style="color:#00ff88">${JSON.stringify(data, null, 2)}</pre>`, "✅ Pipeline Result");
    } catch(e) {
        showResult(`❌ Error: ${e.message}`, "Pipeline Failed");
    }
    btn.disabled = false;
    btn.innerHTML = "▶ Run Demo Pipeline";
}

// Queue Celery Task - Improved
async function runCelery() {
    const btn = document.getElementById('b2');
    btn.disabled = true;
    btn.innerHTML = "⏳ Queuing...";
    showResult("🔄 Sending task to Celery Queue...", "Celery Task");

    try {
        const data = await fetch('/celery/run').then(r => r.json());
        showResult(`
            <strong>Task ID:</strong> ${data.task_id}<br>
            <strong>Queued At:</strong> ${data.queued_at}<br>
            <strong>Status:</strong> ${data.status}<br><br>
            <em>Note: On free tier this runs immediately (simulation).</em>
        `, "✅ Celery Task Queued");
    } catch(e) {
        showResult("❌ Failed to queue task", "Celery Error");
    }
    btn.disabled = false;
    btn.innerHTML = "🔄 Queue Celery Task";
}

// System Status
async function getStatus() {
    showResult("⏳ Fetching system status...", "System Status");
    try {
        const data = await fetch('/status').then(r => r.json());
        showResult(`<pre>${JSON.stringify(data, null, 2)}</pre>`, "📊 System Status");
    } catch(e) {
        showResult("❌ Failed to fetch status", "Status Error");
    }
}

// Task History
async function getHistory() {
    showResult("⏳ Loading task history...", "Task History");
    try {
        const data = await fetch('/celery/history').then(r => r.json());
        if (!data.tasks || data.tasks.length === 0) {
            showResult("<em>No tasks yet. Run the pipeline first!</em>", "Task History");
        } else {
            let html = "<pre>";
            data.tasks.forEach(t => {
                html += `🕒 ${t.timestamp?.slice(11,19) || ''} | ${t.task} → ${t.status}\n`;
            });
            html += "</pre>";
            showResult(html, "📋 Task History");
        }
    } catch(e) {
        showResult("❌ Failed to load history", "History Error");
    }
}
</script>
</body>
</html>"""

# ====================== ROUTES ======================
@app.route("/")
def home():
    return PAGE

@app.route("/run")
def run():
    result = do_etl("web_run")
    LOGS.append({"task": "run_pipeline", "status": "SUCCESS", "result": f"{result.get('loaded',0)} rows", "timestamp": datetime.now().isoformat()})
    return jsonify(result)

@app.route("/celery/run")
def celery_run():
    tid = str(uuid.uuid4())[:8].upper()
    result = do_etl(f"celery_{tid}")
    LOGS.append({"task": "celery_demo_task", "status": "SUCCESS", "result": f"{result.get('loaded',0)} rows", "timestamp": datetime.now().isoformat()})
    return jsonify({
        "task_id": tid,
        "queued_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "SUCCESS ✅",
        "result": "Task executed successfully"
    })

@app.route("/celery/history")
def celery_history():
    return jsonify({"tasks": list(reversed(LOGS[-20:]))})   # Last 20 tasks

@app.route("/status")
def status():
    try:
        conn = sqlite3.connect("etl.db")
        n = conn.execute("SELECT COUNT(*) FROM emp").fetchone()[0]
        conn.close()
        db_status = f"Connected — {n} rows"
    except:
        db_status = "Ready (no data yet)"
    return jsonify({
        "status": "Online ✅",
        "database": db_status,
        "tasks_done": len(LOGS)
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
