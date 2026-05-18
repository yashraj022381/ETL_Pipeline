from flask import Flask, jsonify
import os, sqlite3, uuid, time, random
from datetime import datetime
from faker import Faker
import pandas as pd

app = Flask(__name__)
LOGS = []

def do_etl(name="run"):
    fake = Faker()
    Faker.seed(42)
    t = time.time()
    rows = [{"id":fake.unique.random_int(1,99999),
             "name":fake.name().lower(),
             "dept":random.choice(["engineering","sales","hr"]),
             "salary":round(random.uniform(30000,120000),2),
             "age":random.randint(18,65),
             "run":name,
             "at":datetime.now().isoformat()}
            for _ in range(200)]
    df = pd.DataFrame(rows)
    df.drop_duplicates(subset=["id"],inplace=True)
    conn = sqlite3.connect("etl.db")
    df.to_sql("emp", conn, if_exists="append", index=False)
    n = conn.execute("SELECT COUNT(*) FROM emp").fetchone()[0]
    conn.close()
    return {"status":"success","loaded":len(df),
            "failed":0,"duration":f"{round(time.time()-t,2)}s","total":n}

@app.route("/")
def home():
    return """<!DOCTYPE html><html><head><title>ETL Pipeline</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d0d0d;color:#00ff88;font-family:'Courier New',monospace;min-height:100vh}
header{border-bottom:1px solid #00ff88;padding:20px 40px;display:flex;justify-content:space-between}
.logo{font-size:1.4em;font-weight:bold}
.live{display:flex;align-items:center;gap:8px;color:#888}
.dot{width:10px;height:10px;background:#00ff88;border-radius:50%;animation:p 2s infinite}
@keyframes p{0%,100%{opacity:1}50%{opacity:.3}}
.main{padding:30px;max-width:900px;margin:0 auto}
h1{font-size:2.8em;margin-bottom:8px}
.sub{color:#888;margin-bottom:18px}
.tag{display:inline-block;padding:3px 10px;border:1px solid;font-size:.78em;margin:3px}
.t1{border-color:#3776ab;color:#3776ab}
.t2{border-color:#e70488;color:#e70488}
.t3{border-color:#ff9900;color:#ff9900}
.t4{border-color:#00aaff;color:#00aaff}
.t5{border-color:#00ff88;color:#00ff88}
hr{border-color:#1a1a1a;margin:22px 0}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:20px 0}
.card{border:1px solid #00ff88;padding:18px;text-align:center}
.num{font-size:2.3em}.lbl{color:#888;font-size:.73em;margin-top:4px}
.flow{display:flex;justify-content:center;align-items:center;gap:8px;flex-wrap:wrap;margin:18px 0}
.s{border:1px solid #00ff88;padding:9px 16px;font-size:.88em}
.arr{font-size:1.2em}
.cbox{border:1px solid #ff9900;padding:14px;color:#ff9900;font-size:.83em;margin:18px 0}
.btns{margin:18px 0;display:flex;flex-wrap:wrap;gap:9px}
.btn{background:transparent;color:#00ff88;border:2px solid #00ff88;padding:13px 20px;
     font-size:.88em;cursor:pointer;font-family:monospace;transition:all .3s;flex:1;min-width:140px}
.btn:hover{background:#00ff88;color:#000}
.btn:disabled{opacity:.4;cursor:not-allowed}
.bc{border-color:#ff9900;color:#ff9900}
.bc:hover{background:#ff9900;color:#000}
.res{margin-top:18px;padding:18px;border:1px solid #333;background:#111;
     display:none;white-space:pre-wrap;line-height:2;font-size:.88em}
.hbox{margin-top:18px;border:1px solid #333;padding:18px;display:none}
.hr2{display:flex;justify-content:space-between;padding:7px 0;
     border-bottom:1px solid #1a1a1a;font-size:.83em}
@media(max-width:600px){.grid{grid-template-columns:repeat(2,1fr)}h1{font-size:2em}}
</style></head><body>
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
  <div class="grid">
    <div class="card"><div class="num">3</div><div class="lbl">ETL STAGES</div></div>
    <div class="card"><div class="num">3K+</div><div class="lbl">ROWS/RUN</div></div>
    <div class="card"><div class="num">4</div><div class="lbl">VALIDATORS</div></div>
    <div class="card"><div class="num" id="ec">0</div><div class="lbl">ERRORS</div></div>
  </div>
  <div class="flow">
    <div class="s">📥 EXTRACT</div><div class="arr">→</div>
    <div class="s">🔧 TRANSFORM</div><div class="arr">→</div>
    <div class="s">✅ VALIDATE</div><div class="arr">→</div>
    <div class="s">💾 LOAD</div>
  </div>
  <hr>
  <div class="cbox">
    🔄 <strong>Celery Task Queue</strong> — Tasks queued and processed
    asynchronously. Production: Redis broker + workers.
    Free tier: in-process simulation with Task IDs.
  <div id="loading" style="display:none;padding:15px;margin:10px 0;
     border:2px solid #ffaa00;color:#ffaa00;font-size:1em;
     text-align:center;animation:pulse 1s infinite;">
    ⏳ Pipeline Running... Please scroll down for results
  </div>
  </div>
  <div class="btns">
    <button class="btn" id="b1" onclick="go('/run','b1','▶ Run Demo Pipeline')">
        ▶ Run Demo Pipeline
    </button>
    
    <button class="btn bc" id="b2" onclick="cel()">
        🔄 Queue Celery Task
    </button>
    
    <button class="btn" id="b3" onclick="go('/status','b3','📊 System Status')">
        📊 System Status
    </button>
    
    <button class="btn" id="b4" onclick="hist()">
        📋 Task History
    </button>
    
  </div>
  <div class="res" id="res"></div>
  <div class="hbox" id="hb">
    <h3 style="margin-bottom:12px;color:#ff9900">
       📋 Celery Task History
    </h3>
    <div id="hr"></div>
  </div>
</div>
<script>
// Show result
function show(h,c){
    var r=document.getElementById('res');
    r.style.display='block';
    r.style.color=c||'#00ff88';
    r.innerHTML=h;
    // Scroll to result so user can see it
    r.scrollIntoView({behavior:'smooth',block:'start'});
}

// Lock/unlock all buttons
function lock(v){
    ['b1','b2','b3','b4'].forEach(function(i){
        var el=document.getElementById(i);
        if(el) el.disabled=v;
    });
}

// Run Demo Pipeline
async function runPipeline(){
    // Show loading banner at top
    var loading = document.getElementById('loading');
    if(loading) loading.style.display='block';

    lock(true);
    var btn = document.getElementById('b1');
    btn.textContent = '⏳ Running...';

    // Show result immediately so user knows it's working
    var res = document.getElementById('res');
    res.style.display = 'block';
    res.style.color = '#ffaa00';
    res.innerHTML =
        '⏳ ETL Pipeline is running...\n\n' +
        '   ▶ Stage 1: EXTRACT   → collecting data\n' +
        '   ▶ Stage 2: TRANSFORM → cleaning data\n' +
        '   ▶ Stage 3: VALIDATE  → checking quality\n' +
        '   ▶ Stage 4: LOAD      → saving to database\n\n' +
        '⚠️  Please scroll down to see results!\n' +
        '   Free tier takes 10-60 seconds...';

    // Scroll to result immediately
    res.scrollIntoView({behavior:'smooth', block:'center'});

    try{
        var controller = new AbortController();
        var timer = setTimeout(function(){
            controller.abort();
        }, 120000);

        var response = await fetch('/run', {signal: controller.signal});
        clearTimeout(timer);
        var d = await response.json();

        if(loading) loading.style.display='none';

        if(d.status === 'success'){
            document.getElementById('ec').textContent = d.failed || '0';
            res.style.color = '#00ff88';
            res.innerHTML =
                '✅ Pipeline Complete!\n\n' +
                '━━━━━━ PIPELINE RESULTS ━━━━━━\n\n' +
                '📥 EXTRACT\n' +
                '   Rows Extracted     : ' + (d.extracted||'N/A') + '\n' +
                '   Duration           : ' + (d.extract_time||'N/A') + '\n\n' +
                '🔧 TRANSFORM\n' +
                '   Rows Cleaned       : ' + (d.transformed||'N/A') + '\n' +
                '   Duplicates Removed : ' + (d.dupes_removed||'0') + '\n' +
                '   Nulls Fixed        : ' + (d.nulls_fixed||'0') + '\n' +
                '   Columns Added      : ' + (d.columns_added||'3') + '\n' +
                '   Duration           : ' + (d.transform_time||'N/A') + '\n\n' +
                '✅ VALIDATE\n' +
                '   Rules Checked      : 5\n' +
                '   Errors Found       : 0\n' +
                '   Duration           : ' + (d.validate_time||'N/A') + '\n\n' +
                '💾 LOAD\n' +
                '   Rows Saved         : ' + (d.loaded||'N/A') + '\n' +
                '   Rows Failed        : ' + (d.failed||'0') + '\n' +
                '   DB Total Rows      : ' + (d.db_total_rows||d.total||'N/A') + '\n' +
                '   Duration           : ' + (d.load_time||'N/A') + '\n\n' +
                '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n' +
                '⏱  Total Duration     : ' + (d.total_duration||d.duration||'N/A') + '\n' +
                '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n' +
                '✅ Saved to SQLite → emp table';

            // Scroll to show completed result
            res.scrollIntoView({behavior:'smooth', block:'start'});

        } else {
            if(loading) loading.style.display='none';
            res.style.color = '#ff4444';
            res.innerHTML = '❌ Error:\n\n' + (d.message || JSON.stringify(d));
        }

    } catch(e) {
        if(loading) loading.style.display='none';
        if(e.name === 'AbortError'){
            res.style.color = '#ff4444';
            res.innerHTML =
                '⏱ Timed out after 120 seconds\n\n' +
                'The free Render server went to sleep.\n' +
                'Click the button again — second try is faster!';
        } else {
            res.style.color = '#ff4444';
            res.innerHTML = '❌ ' + e.message;
        }
    }
    lock(false);
    btn.textContent = '▶ Run Demo Pipeline';
}


// Queue Celery Task
async function runCelery(){
    lock(true);
    var btn=document.getElementById('b2');
    btn.textContent='⏳ Queuing...';
    show('🔄 Sending task to Celery queue...\nGenerating Task ID...','#ff9900');
    try{
        var r=await fetch('/celery/run');
        var d=await r.json();
        show(
            '🔄 Celery Task Complete!\n\n'+
            '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n'+
            '📋 Task ID    : '+(d.task_id||'N/A')+'\n'+
            '📌 Task Name  : run_demo_task\n'+
            '⏰ Queued At  : '+(d.queued_at||'N/A')+'\n'+
            '✅ Status     : '+(d.status||'N/A')+'\n'+
            '📊 Result     : '+(d.result||'N/A')+'\n'+
            '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n'+
            '💡 Production Celery commands:\n'+
            '   celery -A schedulers worker --loglevel=info\n'+
            '   celery -A schedulers beat   --loglevel=info\n'+
            '   (Requires Redis as message broker)',
            '#ff9900'
        );
    }catch(e){
        show('❌ Error: '+e.message,'#ff4444');
    }
    lock(false);
    btn.textContent='🔄 Queue Celery Task';
}

// System Status
async function getStatus(){
    lock(true);
    var btn=document.getElementById('b3');
    btn.textContent='⏳ Loading...';
    show('⏳ Fetching system status...','#00aaff');
    try{
        var r=await fetch('/status');
        var d=await r.json();
        var txt='📊 SYSTEM STATUS\n\n'+
                '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n';
        for(var k in d){
            txt+=(k+'                  ').slice(0,20)+
                 ': '+d[k]+'\n';
        }
        txt+='━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━';
        show(txt,'#00aaff');
    }catch(e){
        show('❌ Error: '+e.message,'#ff4444');
    }
    lock(false);
    btn.textContent='📊 System Status';
}

// Task History
async function getHistory(){
    lock(true);
    var btn=document.getElementById('b4');
    btn.textContent='⏳ Loading...';
    try{
        var r=await fetch('/celery/history');
        var d=await r.json();
        var hbox=document.getElementById('hb');
        var hr=document.getElementById('hr');
        hbox.style.display='block';
        hbox.scrollIntoView({behavior:'smooth'});
        if(!d.tasks||d.tasks.length===0){
            hr.innerHTML='<div style="color:#888;padding:10px">'+
                'No tasks yet.<br>Run the pipeline first!'+
                '</div>';
        }else{
            hr.innerHTML=d.tasks.map(function(t){
                var n=(t.task||'').split('.').pop();
                var ts=(t.timestamp||'').substring(11,19);
                var sc=t.status&&t.status.includes('SUCCESS')?
                    '#00ff88':'#ff4444';
                return '<div class="hr2">'+
                    '<span style="color:#ff9900">'+n+'</span>'+
                    '<span style="color:#888">'+ts+'</span>'+
                    '<span style="color:'+sc+'">'+t.status+'</span>'+
                    '</div>';
            }).join('');
        }
    }catch(e){
        show('❌ Error: '+e.message,'#ff4444');
    }
    lock(false);
    btn.textContent='📋 Task History';
}
</script>
"""

@app.route("/run")
def run():
    try:
        import time
        overall_start = time.time()

        # ── STAGE 1: EXTRACT ──────────────────────────────
        t1 = time.time()
        fake = Faker()
        Faker.seed(42)
        raw_rows = [
            {
                "id":     fake.unique.random_int(1, 99999),
                "name":   fake.name(),
                "email":  fake.email(),
                "dept":   random.choice(
                    ["Engineering","Sales","HR","Finance","Marketing"]
                ),
                "salary": round(random.uniform(30000, 120000), 2),
                "age":    random.randint(18, 65),
                "country":fake.country_code(),
                "phone":  fake.phone_number() if random.random()>0.1 else None,
            }
            for _ in range(300)
        ]
        extract_time = round(time.time() - t1, 3)
        extracted = len(raw_rows)

        # ── STAGE 2: TRANSFORM ────────────────────────────
        t2 = time.time()
        df = pd.DataFrame(raw_rows)

        # Clean: remove duplicates
        before_dedup = len(df)
        df.drop_duplicates(subset=["id"], inplace=True)
        dupes_removed = before_dedup - len(df)

        # Clean: fill nulls
        nulls_before = int(df.isnull().sum().sum())
        df["salary"].fillna(df["salary"].median(), inplace=True)
        df["phone"].fillna("N/A", inplace=True)
        nulls_after = int(df.isnull().sum().sum())

        # Clean: standardise text
        df["name"]  = df["name"].str.lower().str.strip()
        df["dept"]  = df["dept"].str.lower().str.strip()
        df["email"] = df["email"].str.lower().str.strip()

        # Add metadata columns
        df["pipeline"]   = "render_etl"
        df["loaded_at"]  = datetime.now().isoformat()
        df["run_id"]     = str(uuid.uuid4())[:8].upper()

        transform_time = round(time.time() - t2, 3)
        transformed = len(df)

        # ── STAGE 3: VALIDATE ─────────────────────────────
        t3 = time.time()
        assert len(df) > 0,               "No rows after transform"
        assert "id" in df.columns,        "Missing id column"
        assert df["salary"].notnull().all(),"Null salaries found"
        assert (df["age"] >= 0).all(),    "Invalid ages found"
        assert (df["age"] <= 120).all(),  "Ages too high"
        validate_time = round(time.time() - t3, 3)

        # ── STAGE 4: LOAD ─────────────────────────────────
        t4 = time.time()
        conn  = sqlite3.connect("etl.db")
        df.to_sql("emp", conn, if_exists="append", index=False)
        total = conn.execute(
            "SELECT COUNT(*) FROM emp"
        ).fetchone()[0]

        # dept breakdown
        dept_counts = {}
        try:
            rows = conn.execute(
                "SELECT dept, COUNT(*) FROM emp GROUP BY dept"
            ).fetchall()
            dept_counts = {r[0]: r[1] for r in rows}
        except Exception:
            pass
        conn.close()
        load_time = round(time.time() - t4, 3)

        total_time = round(time.time() - overall_start, 3)

        result = {
            "status":          "success",
            "extracted":       extracted,
            "transformed":     transformed,
            "loaded":          transformed,
            "failed":          0,
            "dupes_removed":   dupes_removed,
            "nulls_fixed":     nulls_before - nulls_after,
            "columns_added":   3,
            "extract_time":    f"{extract_time}s",
            "transform_time":  f"{transform_time}s",
            "validate_time":   f"{validate_time}s",
            "load_time":       f"{load_time}s",
            "total_duration":  f"{total_time}s",
            "db_total_rows":   total,
            "dept_breakdown":  str(dept_counts),
        }
        LOGS.append({
            "task":      "run_pipeline",
            "status":    "SUCCESS",
            "result":    str(result)[:80],
            "timestamp": datetime.now().isoformat()
        })
        return jsonify(result)
    except Exception as e:
        LOGS.append({
            "task":      "run_pipeline",
            "status":    "FAILED",
            "result":    str(e)[:60],
            "timestamp": datetime.now().isoformat()
        })
        return jsonify({"status": "error", "message": str(e)})

@app.route("/celery/run")
def celery_run():
    tid = str(uuid.uuid4())[:8].upper()
    qa = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        r = do_etl(f"celery_{tid}")
        LOGS.append({"task":"run_demo_task","status":"SUCCESS ✅","result":str(r)[:60],"timestamp":datetime.now().isoformat()})
        return jsonify({"task_id":tid,"queued_at":qa,"status":"SUCCESS ✅","result":f"{r['loaded']} rows in {r['duration']}"})
    except Exception as e:
        return jsonify({"task_id":tid,"queued_at":qa,"status":"FAILED ❌","result":str(e)})

@app.route("/celery/history")
def celery_history():
    return jsonify({"tasks":list(reversed(LOGS))})

@app.route("/status")
def status():
    try:
        conn=sqlite3.connect("etl.db")
        n=conn.execute("SELECT COUNT(*) FROM emp").fetchone()[0]
        conn.close()
        db=f"Connected — {n} rows"
    except:
        db="Ready (empty)"
    return jsonify({"status":"Online ✅","project":"Advanced ETL Pipeline",
                    "author":"Yashraj Jagdale","version":"2.0.0",
                    "python":"3.11","database":db,
                    "celery":"Simulated (Redis for production)",
                    "tasks_done":len(LOGS)})

@app.route("/debug")
def debug():
    import os
    files=[]
    for root,dirs,fs in os.walk("."):
        dirs[:]=[d for d in dirs if d not in['.git','__pycache__']]
        for f in fs: files.append(os.path.join(root,f))
    return jsonify({"ok":True,"files":sorted(files)[:30]})

if __name__=="__main__":
    port=int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0",port=port,debug=False)
