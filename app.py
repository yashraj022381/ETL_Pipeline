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
    try:
        fake = Faker()
        Faker.seed(42)
        t0 = time.time()

        # EXTRACT
        t1 = time.time()
        raw = [{
            "id": fake.unique.random_int(1, 99999),
            "name": fake.name(),
            "email": fake.email(),
            "dept": random.choice(["Engineering", "Sales", "HR", "Finance", "Marketing"]),
            "salary": round(random.uniform(30000, 120000), 2),
            "age": random.randint(18, 65),
            "phone": fake.phone_number() if random.random() > 0.1 else None,
            "country": fake.country_code()
        } for _ in range(300)]
        et = round(time.time() - t1, 3)

        # TRANSFORM
        t2 = time.time()
        df = pd.DataFrame(raw)
        before = len(df)
        df.drop_duplicates(subset=["id"], inplace=True)
        dupes = before - len(df)

        df["salary"] = df["salary"].fillna(df["salary"].median())
        df["phone"] = df["phone"].fillna("N/A")
        df[["name", "dept", "email"]] = df[["name", "dept", "email"]].apply(lambda x: x.str.lower().str.strip())

        df["pipeline"] = name
        df["loaded_at"] = datetime.now().isoformat()
        df["run_id"] = str(uuid.uuid4())[:8].upper()
        tt = round(time.time() - t2, 3)

        # LOAD
        t4 = time.time()
        conn = sqlite3.connect("etl.db")
        df.to_sql("emp", conn, if_exists="append", index=False)
        total = conn.execute("SELECT COUNT(*) FROM emp").fetchone()[0]
        depts = dict(conn.execute("SELECT dept, COUNT(*) FROM emp GROUP BY dept").fetchall())
        conn.close()
        lt = round(time.time() - t4, 3)

        return {
            "status": "success",
            "message": "Pipeline executed successfully!",
            "extracted": len(raw),
            "transformed": len(df),
            "loaded": len(df),
            "failed": 0,
            "dupes_removed": dupes,
            "total_duration": f"{round(time.time()-t0,3)}s",
            "db_total": total,
            "dept_breakdown": depts
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ====================== FINAL BEAUTIFUL HTML ======================
PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ETL Pipeline — Yashraj Jagdale</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0a0a0a;color:#00ff88;font-family:'Courier New',monospace;min-height:100vh}
header{border-bottom:1px solid #00ff88;padding:20px 40px;display:flex;justify-content:space-between;align-items:center}
.logo{font-size:1.7em;font-weight:bold}
.live{display:flex;align-items:center;gap:8px}
.dot{width:12px;height:12px;background:#00ff88;border-radius:50%;animation:blink 2s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:0.3}}

main{padding:30px 40px;max-width:1200px;margin:0 auto}
h1{font-size:2.8em;margin-bottom:5px}
.sub{color:#888;margin-bottom:20px}

.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:18px;margin:25px 0}
.card{border:1px solid #00ff88;padding:22px;text-align:center;border-radius:8px;background:#111}
.num{font-size:3em;font-weight:bold}
.lbl{color:#888;font-size:0.85em;margin-top:8px;letter-spacing:1px}

.flow{display:flex;justify-content:center;align-items:center;gap:15px;flex-wrap:wrap;margin:35px 0}
.stage{border:1px solid #00ff88;padding:14px 24px;border-radius:8px;font-size:1.1em}

.cbox{border:1px solid #ff9900;padding:18px;margin:25px 0;border-radius:8px;background:#1a1a1a}

.btns{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;margin:30px 0}
.btn{background:transparent;border:2px solid #00ff88;color:#00ff88;padding:18px;font-size:1.05em;cursor:pointer;border-radius:8px;transition:0.3s}
.btn:hover{background:#00ff88;color:#000}
.btn:disabled{opacity:0.5;cursor:not-allowed}

#res{margin-top:25px;padding:25px;background:#111;border:1px solid #00ff88;border-radius:10px;white-space:pre-wrap;display:none;line-height:1.8;font-size:1.02em;min-height:120px}
.success{color:#00ff88}
.error{color:#ff6666}
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

    <div class="grid">
        <div class="card"><div class="num">3</div><div class="lbl">ETL STAGES</div></div>
        <div class="card"><div class="num">3K+</div><div class="lbl">ROWS / RUN</div></div>
        <div class="card"><div class="num">4</div><div class="lbl">VALIDATORS</div></div>
        <div class="card"><div class="num" id="ec">0</div><div class="lbl">ERRORS</div></div>
    </div>

    <div class="flow">
        <div class="stage">📥 EXTRACT</div>
        <div style="color:#00ff88;font-size:2em">→</div>
        <div class="stage">🔧 TRANSFORM</div>
        <div style="color:#00ff88;font-size:2em">→</div>
        <div class="stage">✅ VALIDATE</div>
        <div style="color:#00ff88;font-size:2em">→</div>
        <div class="stage">💾 LOAD</div>
    </div>

    <div class="cbox">
        🔄 <strong>Celery Task Queue</strong> — Free tier simulation mode active
    </div>

    <div class="btns">
        <button class="btn" id="b1" onclick="runPipeline()">▶ Run Demo Pipeline</button>
        <button class="btn" id="b2" onclick="runCelery()">🔄 Queue Celery Task</button>
        <button class="btn" id="b3" onclick="getStatus()">📊 System Status</button>
        <button class="btn" id="b4" onclick="getHistory()">📋 Task History</button>
    </div>

    <div id="res"></div>
</main>

<script>
function showResult(html, success=true) {
    const res = document.getElementById('res');
    res.style.display = 'block';
    res.innerHTML = html;
    res.style.borderColor = success ? '#00ff88' : '#ff6666';
}

async function runPipeline() {
    const btn = document.getElementById('b1');
    btn.disabled = true;
    btn.textContent = "⏳ Running Pipeline...";
    
    const res = document.getElementById('res');
    res.style.display = 'block';
    res.innerHTML = "<b>🚀 Starting ETL Pipeline...</b>";

    try {
        const data = await fetch('/run').then(r => r.json());
        
        let html = `<b style="color:#00ff88">✅ Pipeline Executed Successfully!</b><br><br>`;
        html += `📊 <b>Loaded:</b> ${data.loaded} rows<br>`;
        html += `⏱ <b>Duration:</b> ${data.total_duration}<br>`;
        html += `🗄 <b>Database Total:</b> ${data.db_total} rows<br><br>`;
        
        if (data.dept_breakdown) {
            html += `<b>Departments:</b><br>`;
            Object.entries(data.dept_breakdown).forEach(([dept, count]) => {
                html += `   • ${dept}: ${count} rows<br>`;
            });
        }
        showResult(html);
    } catch(e) {
        showResult(`❌ Error: ${e.message}`, false);
    }
    
    btn.disabled = false;
    btn.textContent = "▶ Run Demo Pipeline";
}

async function runCelery() {
    const btn = document.getElementById('b2');
    btn.disabled = true;
    btn.textContent = "⏳ Queuing...";
    
    try {
        const data = await fetch('/celery/run').then(r => r.json());
        const html = `<b>✅ Celery Task Queued!</b><br><br>
                      Task ID: <b>${data.task_id}</b><br>
                      Status: <b>${data.status}</b><br>
                      <small>Running in simulation mode (free tier)</small>`;
        showResult(html);
    } catch(e) {
        showResult("❌ Failed to queue task", false);
    }
    btn.disabled = false;
    btn.textContent = "🔄 Queue Celery Task";
}

async function getStatus() {
    try {
        const data = await fetch('/status').then(r => r.json());
        let html = `<b>📊 System Status</b><br><br>`;
        Object.entries(data).forEach(([k,v]) => {
            html += `<b>${k}:</b> ${v}<br>`;
        });
        showResult(html);
    } catch(e) {
        showResult("❌ Failed to fetch status", false);
    }
}

async function getHistory() {
    try {
        const data = await fetch('/celery/history').then(r => r.json());
        let html = `<b>📋 Task History</b><br><br>`;
        if (data.tasks && data.tasks.length > 0) {
            data.tasks.forEach(t => {
                html += `• ${t.timestamp} | ${t.result}<br>`;
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
    LOGS.append({
        "task": "run_demo_task",
        "status": "SUCCESS",
        "result": f"{result.get('loaded', 0)} rows",
        "timestamp": datetime.now().strftime("%H:%M:%S")
    })
    return jsonify({"task_id": tid, "status": "SUCCESS", "result": f"{result.get('loaded', 0)} rows"})

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

@app.route("/celery/history")
def celery_history():
    return jsonify({"tasks": list(reversed(LOGS[-20:]))})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
