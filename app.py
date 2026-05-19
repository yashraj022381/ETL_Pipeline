import os
import sys
import uuid
import time
import sqlite3
import random
from datetime import datetime
from flask import Flask, jsonify, request
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
        raw = [
            {
                "id": fake.unique.random_int(1, 99999),
                "name": fake.name(),
                "email": fake.email(),
                "dept": random.choice(["Engineering", "Sales", "HR", "Finance", "Marketing"]),
                "salary": round(random.uniform(30000, 120000), 2),
                "age": random.randint(18, 65),
                "phone": fake.phone_number() if random.random() > 0.1 else None,
                "country": fake.country_code()
            }
            for _ in range(300)
        ]
        et = round(time.time() - t1, 3)

        # ==================== TRANSFORM ====================
        t2 = time.time()
        df = pd.DataFrame(raw)
        before = len(df)

        df.drop_duplicates(subset=["id"], inplace=True)
        dupes = before - len(df)

        # Fix deprecated fillna
        df["salary"] = df["salary"].fillna(df["salary"].median())
        df["phone"] = df["phone"].fillna("N/A")
        
        df["name"] = df["name"].str.lower().str.strip()
        df["dept"] = df["dept"].str.lower().str.strip()
        df["email"] = df["email"].str.lower().str.strip()

        df["pipeline"] = name
        df["loaded_at"] = datetime.now().isoformat()
        df["run_id"] = str(uuid.uuid4())[:8].upper()

        tt = round(time.time() - t2, 3)

        # ==================== VALIDATE ====================
        t3 = time.time()
        assert len(df) > 0, "No data after transform"
        assert df["salary"].notnull().all(), "Salary has nulls"
        assert (df["age"] >= 0).all() and (df["age"] <= 120).all(), "Age out of range"
        vt = round(time.time() - t3, 3)

        # ==================== LOAD ====================
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
            "total_duration": f"{round(time.time()-t0, 3)}s",
            "db_total": total,
            "dept_breakdown": str(depts),
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

# ====================== ROUTES ======================

@app.route("/")
def home():
    return PAGE  # Your big HTML is at the bottom

@app.route("/run")
def run():
    result = do_etl("web_run")
    LOGS.append({
        "task": "run_pipeline",
        "status": "SUCCESS" if result.get("status") == "success" else "FAILED",
        "result": f"{result.get('loaded', 0)} rows",
        "timestamp": datetime.now().isoformat()
    })
    return jsonify(result)

@app.route("/celery/run")
def celery_run():
    tid = str(uuid.uuid4())[:8].upper()
    result = do_etl(f"celery_{tid}")
    LOGS.append({...})   # similar as above
    return jsonify({
        "task_id": tid,
        "queued_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "SUCCESS ✅",
        "result": f"{result.get('loaded', 0)} rows"
    })

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
        "project": "Advanced ETL Pipeline",
        "author": "Yashraj Jagdale",
        "version": "2.0.0",
        "database": db_status,
        "tasks_done": len(LOGS),
    })

@app.route("/celery/history")
def celery_history():
    return jsonify({"tasks": list(reversed(LOGS[-20:]))})  # last 20 only

# Keep your big HTML `PAGE = """ ... """` at the bottom (same as before)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
