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
LOGS = []

def do_etl(name="run"):
    # ... [Keep your do_etl function same as before - no change needed] ...
    fake = Faker()
    Faker.seed(42)
    t0 = time.time()

    try:
        t1 = time.time()
        raw = [{ ... }]   # Keep your existing data generation
        # ... rest of your do_etl function (same as last version) ...
        return { ... }    # Keep returning the same dictionary
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ====================== BEAUTIFUL HTML + IMPROVED JS ======================
PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ETL Pipeline — Yashraj Jagdale</title>
<style>
/* Your existing beautiful CSS - keeping it short */
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d0d0d;color:#00ff88;font-family:'Courier New',monospace;min-height:100vh}
header{border-bottom:1px solid #00ff88;padding:18px 35px;display:flex;justify-content:space-between}
.logo{font-size:1.6em;font-weight:bold}
.live{display:flex;align-items:center;gap:8px}
.dot{width:10px;height:10px;background:#00ff88;border-radius:50%;animation:blink 2s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:0.3}}

main{padding:30px 35px;max-width:1100px;margin:0 auto}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin:20px 0}
.card{border:1px solid #00ff88;padding:20px;text-align:center;border-radius:8px}
.num{font-size:2.8em;font-weight:bold}
.lbl{color:#888;font-size:0.85em;margin-top:8px}

.flow{display:flex;justify-content:center;align-items:center;gap:15px;flex-wrap:wrap;margin:25px 0}
.stage{border:1px solid #00ff88;padding:12px 22px;border-radius:6px}

.cbox{border:1px solid #ff9900;padding:18px;margin:25px 0;border-radius:6px;background:#1a1a1a}

.btns{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin:30px 0}
.btn{background:transparent;border:2px solid #00ff88;color:#00ff88;padding:16px;font-size:1.05em;
     cursor:pointer;border-radius:6px;transition:0.3s}
.btn:hover{background:#00ff88;color:#000}

#res{margin-top:25px;padding:25px;background:#111;border:1px solid #00ff88;white-space:pre-wrap;
     border-radius:8px;line-height:1.7;display:none;font-size:0.95em}
.success{color:#00ff88}
.error{color:#ff4444}
</style>
</head>
<body>

<header>
    <div class="logo">⚡ ETL_Pipeline</div>
    <div class="live"><div class="dot"></div> Live on Render</div>
</header>

<main>
    <h1>⚡ ETL Pipeline</h1>
    <p>Advanced Data Engineering Project by Yashraj Jagdale</p>

    <!-- Your stats grid and flow here (same as before) -->

    <div class="btns">
        <button class="btn" id="b1" onclick="runPipeline()">▶ Run Demo Pipeline</button>
        <button class="btn" id="b2" onclick="runCelery()">🔄 Queue Celery Task</button>
        <button class="btn" id="b3" onclick="getStatus()">📊 System Status</button>
        <button class="btn" id="b4" onclick="getHistory()">📋 Task History</button>
    </div>

    <div id="res"></div>
</main>

<script>
function showResult(html, isSuccess = true) {
    const res = document.getElementById('res');
    res.style.display = 'block';
    res.innerHTML = html;
    res.className = isSuccess ? 'success' : 'error';
}

// Run Demo Pipeline
async function runPipeline() {
    const btn = document.getElementById('b1');
    const res = document.getElementById('res');
    btn.disabled = true;
    btn.textContent = "⏳ Running...";
    res.innerHTML = "<b>🚀 Pipeline Running...</b>";

    try {
        const data = await fetch('/run').then(r => r.json());
        
        let html = `<b class="success">✅ Pipeline Executed Successfully!</b><br><br>`;
        html += `<b>Extracted:</b> ${data.extracted} rows<br>`;
        html += `<b>Transformed:</b> ${data.transformed} rows<br>`;
        html += `<b>Loaded:</b> ${data.loaded} rows<br>`;
        html += `<b>Total Duration:</b> ${data.total_duration}<br>`;
        html += `<b>Database Total:</b> ${data.db_total} rows<br><br>`;
        html += `<b>Departments:</b> ${data.dept_breakdown}`;
        
        showResult(html);
    } catch(e) {
        showResult(`❌ Error: ${e.message}`, false);
    }
    
    btn.disabled = false;
    btn.textContent = "▶ Run Demo Pipeline";
}

// Celery Task
async function runCelery() {
    const btn = document.getElementById('b2');
    btn.disabled = true;
    btn.textContent = "⏳ Queuing...";
    
    try {
        const data = await fetch('/celery/run').then(r => r.json());
        const html = `
            <b>✅ Celery Task Queued!</b><br><br>
            <b>Task ID:</b> ${data.task_id}<br>
            <b>Status:</b> ${data.status}<br>
            <b>Result:</b> ${data.result}<br><br>
            <small>Note: Running in simulation mode on free tier.</small>
        `;
        showResult(html);
    } catch(e) {
        showResult("❌ Failed to queue task", false);
    }
    btn.disabled = false;
    btn.textContent = "🔄 Queue Celery Task";
}

// System Status
async function getStatus() {
    try {
        const data = await fetch('/status').then(r => r.json());
        let html = `<b>📊 System Status</b><br><br>`;
        for (let [key, value] of Object.entries(data)) {
            html += `<b>${key}:</b> ${value}<br>`;
        }
        showResult(html);
    } catch(e) {
        showResult("❌ Failed to get status", false);
    }
}

// Task History
async function getHistory() {
    try {
        const data = await fetch('/celery/history').then(r => r.json());
        let html = `<b>📋 Task History</b><br><br>`;
        
        if (data.tasks && data.tasks.length > 0) {
            data.tasks.forEach(task => {
                html += `• ${task.timestamp} | ${task.task} → ${task.status} (${task.result})<br>`;
            });
        } else {
            html += "No tasks yet. Run the pipeline first!";
        }
        showResult(html);
    } catch(e) {
        showResult("❌ Failed to load history", false);
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
    return jsonify(result)

@app.route("/celery/run")
def celery_run():
    tid = str(uuid.uuid4())[:8].upper()
    result = do_etl(f"celery_{tid}")
    LOGS.append({"task": "run_demo_task", "status": "SUCCESS", "result": f"{result.get('loaded',0)} rows", "timestamp": datetime.now().strftime("%H:%M:%S")})
    return jsonify({"task_id": tid, "status": "SUCCESS", "result": f"{result.get('loaded',0)} rows"})

@app.route("/celery/history")
def celery_history():
    return jsonify({"tasks": list(reversed(LOGS[-15:]))})

@app.route("/status")
def status():
    try:
        conn = sqlite3.connect("etl.db")
        n = conn.execute("SELECT COUNT(*) FROM emp").fetchone()[0]
        conn.close()
        db = f"Connected — {n} rows"
    except:
        db = "Ready (no data yet)"
    return jsonify({"status": "Online ✅", "database": db, "tasks_done": len(LOGS)})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
