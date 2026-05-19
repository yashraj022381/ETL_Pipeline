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
        df["name"] = df["name"].str.lower().str.strip()
        df["dept"] = df["dept"].str.lower().str.strip()
        df["email"] = df["email"].str.lower().str.strip()

        df["pipeline"] = name
        df["loaded_at"] = datetime.now().isoformat()
        df["run_id"] = str(uuid.uuid4())[:8].upper()
        tt = round(time.time() - t2, 3)

        # VALIDATE
        t3 = time.time()
        assert len(df) > 0
        assert df["salary"].notnull().all()
        assert (df["age"] >= 0).all() and (df["age"] <= 120).all()
        vt = round(time.time() - t3, 3)

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
            "extracted": len(raw),
            "transformed": len(df),
            "loaded": len(df),
            "failed": 0,
            "dupes_removed": dupes,
            "nulls_fixed": int(df.isnull().sum().sum()),
            "extract_time": f"{et}s",
            "transform_time": f"{tt}s",
            "validate_time": f"{vt}s",
            "load_time": f"{lt}s",
            "total_duration": f"{round(time.time()-t0,3)}s",
            "db_total": total,
            "dept_breakdown": str(depts),
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

# ====================== HTML DASHBOARD ======================
PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ETL Pipeline — Yashraj</title>
<style>
/* Keep your original beautiful CSS here - I kept it short for now */
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d0d0d;color:#00ff88;font-family:'Courier New',monospace}
header{border-bottom:1px solid #00ff88;padding:20px;display:flex;justify-content:space-between}
.btn{background:transparent;border:2px solid #00ff88;color:#00ff88;padding:15px;margin:5px;cursor:pointer}
.btn:hover{background:#00ff88;color:#000}
</style>
</head>
<body>
<header>
    <div>⚡ ETL Pipeline</div>
</header>
<main style="padding:30px">
    <h1>Advanced ETL Pipeline</h1>
    <button class="btn" onclick="runPipeline()">▶ Run Demo Pipeline</button>
    <button class="btn" onclick="getStatus()">📊 Status</button>
    
    <div id="res" style="margin-top:20px;white-space:pre-wrap"></div>
</main>

<script>
async function runPipeline() {
    const res = document.getElementById('res');
    res.innerHTML = "⏳ Running ETL...";
    try {
        const r = await fetch('/run').then(res => res.json());
        res.innerHTML = JSON.stringify(r, null, 2);
    } catch(e) {
        res.innerHTML = "❌ Error: " + e.message;
    }
}

async function getStatus() {
    const res = document.getElementById('res');
    try {
        const r = await fetch('/status').then(res => res.json());
        res.innerHTML = JSON.stringify(r, null, 2);
    } catch(e) {
        res.innerHTML = "❌ " + e.message;
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

@app.route("/status")
def status():
    try:
        conn = sqlite3.connect("etl.db")
        n = conn.execute("SELECT COUNT(*) FROM emp").fetchone()[0]
        conn.close()
        db = f"Connected — {n} rows"
    except:
        db = "Ready (no data yet)"
    return jsonify({"status": "Online", "database": db, "tasks": len(LOGS)})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
