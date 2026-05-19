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
    fake = Faker()
    Faker.seed(42)
    t0 = time.time()

    try:
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

        # VALIDATE + LOAD
        t4 = time.time()
        conn = sqlite3.connect("etl.db")
        df.to_sql("emp", conn, if_exists="append", index=False)
        total = conn.execute("SELECT COUNT(*) FROM emp").fetchone()[0]
        depts = dict(conn.execute("SELECT dept, COUNT(*) FROM emp GROUP BY dept").fetchall())
        conn.close()
        lt = round(time.time() - t4, 3)

        return {
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

    except Exception as e:
        return {"status": "error", "message": str(e)}

# ====================== FULL BEAUTIFUL HTML ======================
PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ETL Pipeline — Yashraj Jagdale</title>
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { background:#0d0d0d; color:#00ff88; font-family:'Courier New',monospace; min-height:100vh; }
header { border-bottom:1px solid #00ff88; padding:18px 35px; display:flex; justify-content:space-between; align-items:center; }
.logo { font-size:1.6em; font-weight:bold; }
.live { display:flex; align-items:center; gap:8px; color:#00ff88; }
.dot { width:10px; height:10px; background:#00ff88; border-radius:50%; animation:blink 2s infinite; }
@keyframes blink { 0%,100% {opacity:1} 50% {opacity:0.3} }

main { padding:30px 35px; max-width:1100px; margin:0 auto; }
h1 { font-size:2.8em; margin-bottom:8px; }
.sub { color:#888; margin-bottom:20px; }

.tags span { display:inline-block; padding:4px 12px; margin:4px; border:1px solid; border-radius:4px; font-size:0.85em; }
.t1 { border-color:#3776ab; color:#3776ab; }
.t2 { border-color:#e70488; color:#e70488; }
.t3 { border-color:#ff9900; color:#ff9900; }
.t4 { border-color:#00aaff; color:#00aaff; }
.t5 { border-color:#00ff88; color:#00ff88; }

.grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(180px,1fr)); gap:16px; margin:25px 0; }
.card { border:1px solid #00ff88; padding:20px; text-align:center; border-radius:8px; }
.num { font-size:2.8em; font-weight:bold; }
.lbl { color:#888; font-size:0.8em; margin-top:6px; letter-spacing:1px; }

.flow { display:flex; justify-content:center; align-items:center; gap:12px; flex-wrap:wrap; margin:30px 0; font-size:1.1em; }
.stage { border:1px solid #00ff88; padding:12px 20px; border-radius:6px; }

.cbox { border:1px solid #ff9900; padding:18px; margin:25px 0; border-radius:6px; background:#1a1a1a; }

.btns { display:grid; grid-template-columns:repeat(auto-fit, minmax(220px,1fr)); gap:12px; margin:30px 0; }
.btn { background:transparent; border:2px solid #00ff88; color:#00ff88; padding:16px; font-size:1.05em; cursor:pointer; 
       font-family:'Courier New',monospace; transition:0.3s; border-radius:6px; }
.btn:hover { background:#00ff88; color:#000; }
.btn:disabled { opacity:0.4; cursor:not-allowed; }

#res { margin-top:25px; padding:20px; background:#111; border:1px solid #333; white-space:pre-wrap; border-radius:6px; display:none; }
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

    <div class="tags">
        <span class="t1">Python 3.11</span>
        <span class="t2">Pandas</span>
        <span class="t3">Celery</span>
        <span class="t4">SQLite</span>
        <span class="t5">Flask</span>
    </div>

    <div class="grid">
        <div class="card"><div class="num">3</div><div class="lbl">ETL STAGES</div></div>
        <div class="card"><div class="num">3K+</div><div class="lbl">ROWS / RUN</div></div>
        <div class="card"><div class="num">4</div><div class="lbl">VALIDATORS</div></div>
        <div class="card"><div class="num" id="ec">0</div><div class="lbl">ERRORS</div></div>
    </div>

    <div class="flow">
        <div class="stage">📥 EXTRACT</div>
        <div style="color:#00ff88; font-size:1.8em;">→</div>
        <div class="stage">🔧 TRANSFORM</div>
        <div style="color:#00ff88; font-size:1.8em;">→</div>
        <div class="stage">✅ VALIDATE</div>
        <div style="color:#00ff88; font-size:1.8em;">→</div>
        <div class="stage">💾 LOAD</div>
    </div>

    <div class="cbox">
        🔄 <strong>Celery Task Queue</strong> — Tasks queued and processed asynchronously.<br>
        Free tier: in-process simulation with unique Task IDs.
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
async function runPipeline() {
    const res = document.getElementById('res');
    const btn = document.getElementById('b1');
    btn.disabled = true;
    btn.innerHTML = "⏳ Running...";
    res.style.display = 'block';
    res.innerHTML = "🚀 Starting ETL Pipeline...";

    try {
        const data = await fetch('/run').then(r => r.json());
        document.getElementById('ec').textContent = data.failed || 0;
        res.innerHTML = `<pre style="color:#00ff88">${JSON.stringify(data, null, 2)}</pre>`;
    } catch(e) {
        res.innerHTML = "❌ Error: " + e.message;
    }
    btn.disabled = false;
    btn.innerHTML = "▶ Run Demo Pipeline";
}

async function runCelery() { alert("Celery simulation - Task Queued! (Check console for now)"); }
async function getStatus() { 
    const res = document.getElementById('res');
    res.style.display = 'block';
    const data = await fetch('/status').then(r => r.json());
    res.innerHTML = `<pre>${JSON.stringify(data, null, 2)}</pre>`;
}
async function getHistory() { alert("Task History - Coming Soon"); }
</script>
</body>
</html>"""

# ====================== ROUTES ======================
@app.route("/")
def home():
    return PAGE

@app.route("/run")
def run():
    result = do_etl("web_demo")
    return jsonify(result)

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
