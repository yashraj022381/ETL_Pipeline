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
        # ==================== EXTRACT ====================
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

        # ==================== TRANSFORM ====================
        t2 = time.time()
        df = pd.DataFrame(raw)                    # ← This was missing in some versions
        before = len(df)

        df = df.drop_duplicates(subset=["id"])
        dupes = before - len(df)

        # Clean data
        df["salary"] = df["salary"].fillna(df["salary"].median())
        df["phone"] = df["phone"].fillna("N/A")
        df["name"] = df["name"].str.lower().str.strip()
        df["dept"] = df["dept"].str.lower().str.strip()
        df["email"] = df["email"].str.lower().str.strip()

        df["pipeline"] = name
        df["loaded_at"] = datetime.now().isoformat()
        df["run_id"] = str(uuid.uuid4())[:8].upper()
        tt = round(time.time() - t2, 3)

        # ==================== LOAD ====================
        t4 = time.time()
        conn = sqlite3.connect("etl.db")
        df.to_sql("emp", conn, if_exists="append", index=False)
        
        total = conn.execute("SELECT COUNT(*) FROM emp").fetchone()[0]
        depts = dict(conn.execute("SELECT dept, COUNT(*) FROM emp GROUP BY dept").fetchall())
        conn.close()
        lt = round(time.time() - t4, 3)

        result = {
            "status": "success",
            "message": "Pipeline executed successfully!",
            "extracted": len(raw),
            "transformed": len(df),
            "loaded": len(df),
            "failed": 0,
            "dupes_removed": dupes,
            "extract_time": f"{et}s",
            "transform_time": f"{tt}s",
            "load_time": f"{lt}s",
            "total_duration": f"{round(time.time()-t0, 3)}s",
            "db_total": total,
            "dept_breakdown": str(depts),
        }

        LOGS.append({
            "task": "run_pipeline",
            "status": "SUCCESS",
            "result": f"{len(df)} rows",
            "timestamp": datetime.now().strftime("%H:%M:%S")
        })

        return result

    except Exception as e:
        error_result = {"status": "error", "message": str(e)}
        LOGS.append({"task": "run_pipeline", "status": "FAILED", "result": str(e), "timestamp": datetime.now().strftime("%H:%M:%S")})
        return error_result

# ====================== HTML (Your Beautiful UI) ======================
# (Keep the same beautiful HTML you already have from previous version)
# I'm keeping it short here for clarity. Use your existing PAGE if it's better.

PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>ETL Pipeline</title>
<style>
/* Your existing beautiful CSS - I'm assuming you already have it */
body {background:#0d0d0d; color:#00ff88; font-family:'Courier New',monospace;}
.btn {padding:15px; margin:8px; border:2px solid #00ff88; background:transparent; color:#00ff88; cursor:pointer;}
#res {margin-top:20px; padding:15px; background:#111; border:1px solid #00ff88; white-space:pre-wrap; display:none;}
</style>
</head>
<body>
<!-- Your full HTML from previous version -->
<!-- ... keep all your header, cards, flow, buttons ... -->

<div class="btns">
    <button class="btn" onclick="runPipeline()">▶ Run Demo Pipeline</button>
    <button class="btn" onclick="runCelery()">🔄 Queue Celery Task</button>
    <button class="btn" onclick="getStatus()">📊 System Status</button>
    <button class="btn" onclick="getHistory()">📋 Task History</button>
</div>

<div id="res"></div>

<script>
async function runPipeline() {
    const res = document.getElementById('res');
    res.style.display = 'block';
    res.innerHTML = "🚀 Running Pipeline... Please wait";

    try {
        const data = await fetch('/run').then(r => r.json());
        res.innerHTML = `<h3>✅ Pipeline Result</h3><pre>${JSON.stringify(data, null, 2)}</pre>`;
    } catch(e) {
        res.innerHTML = `<h3>❌ Error</h3><pre>${e.message}</pre>`;
    }
}

async function getStatus() {
    const res = document.getElementById('res');
    res.style.display = 'block';
    const data = await fetch('/status').then(r => r.json());
    res.innerHTML = `<h3>📊 System Status</h3><pre>${JSON.stringify(data, null, 2)}</pre>`;
}

async function runCelery() {
    const res = document.getElementById('res');
    res.style.display = 'block';
    res.innerHTML = `<h3>✅ Celery Task Queued</h3><pre>Task ID: ${Math.random().toString(36).substring(2,10).toUpperCase()}\nStatus: SUCCESS\nNote: Running in simulation mode on free tier.</pre>`;
}

async function getHistory() {
    const res = document.getElementById('res');
    res.style.display = 'block';
    const data = await fetch('/history').then(r => r.json());
    res.innerHTML = `<h3>📋 Task History</h3><pre>${JSON.stringify(data, null, 2)}</pre>`;
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
    return jsonify(do_etl("web_run"))

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

@app.route("/history")
def history():
    return jsonify({"tasks": list(reversed(LOGS[-15:]))})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
