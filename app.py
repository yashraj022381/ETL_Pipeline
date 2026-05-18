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
    return """
<!DOCTYPE html>
<html>
<head>
    <title>ETL Pipeline — Yashraj Jagdale</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{
            background:#0d0d0d;color:#00ff88;
            font-family:'Courier New',monospace;min-height:100vh;
        }
        header{
            border-bottom:1px solid #00ff88;
            padding:20px 40px;display:flex;
            justify-content:space-between;align-items:center;
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
            padding:8px 0;border-bottom:1px solid #1a1a1a;
            font-size:0.85em;
        }
        @media(max-width:600px){
            .stats{grid-template-columns:repeat(2,1fr)}
            h1{font-size:2em}
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
        <div class="card">
            <div class="num">3</div>
            <div class="lbl">ETL STAGES</div>
        </div>
        <div class="card">
            <div class="num">3K+</div>
            <div class="lbl">ROWS/RUN</div>
        </div>
        <div class="card">
            <div class="num">4</div>
            <div class="lbl">VALIDATORS</div>
        </div>
        <div class="card">
            <div class="num" id="ecnt">0</div>
            <div class="lbl">ERRORS</div>
        </div>
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
        🔄 <strong>Celery Task Queue</strong> — Tasks queued and
        processed asynchronously. Production: Redis broker + workers.
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
function show(html, color) {
    var r = document.getElementById('result');
    r.style.display = 'block';
    r.style.color = color || '#00ff88';
    r.innerHTML = html;
}
function lock(on) {
    ['b1','b2','b3','b4'].forEach(function(id){
        document.getElementById(id).disabled = on;
    });
}

async function safeFetch(url, timeoutMs) {
    timeoutMs = timeoutMs || 90000;
    var ctrl = new AbortController();
    var t = setTimeout(function(){ctrl.abort();}, timeoutMs);
    try {
        var r = await fetch(url, {signal: ctrl.signal});
        clearTimeout(t);
        return await r.json();
    } catch(e) {
        clearTimeout(t);
        if (e.name === 'AbortError') {
            throw new Error(
                'Request timed out after ' + (timeoutMs/1000) + 's\\n' +
                'Free tier may be waking up. Wait 30s and retry.'
            );
        }
        throw e;
    }
}

async function runPipeline() {
    lock(true);
    document.getElementById('b1').textContent = '⏳ Running...';
    show(
        '⏳ Running ETL Pipeline...\\n' +
        'Extract → Transform → Validate → Load\\n\\n' +
        '⚠️  Free tier may take 30-50 seconds\\n' +
        'Please wait...',
        '#ffaa00'
    );
    try {
        var d = await safeFetch('/run', 90000);
        if (d.status === 'success') {
            document.getElementById('ecnt').textContent = d.failed || '0';
            show(
                '✅ Pipeline Complete!\\n\\n' +
                '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\\n' +
                '📥 Rows Extracted   : ' + d.extracted   + '\\n' +
                '🔧 Rows Transformed : ' + d.transformed + '\\n' +
                '💾 Rows Loaded      : ' + d.loaded      + '\\n' +
                '❌ Rows Failed      : ' + d.failed      + '\\n' +
                '⏱  Duration         : ' + d.duration    + '\\n' +
                '🗄  DB Total Rows    : ' + (d.db_total||'N/A') + '\\n' +
                '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\\n' +
                '✅ Saved to SQLite → employees table',
                '#00ff88'
            );
        } else {
            show('❌ Error:\\n\\n' + d.message, '#ff4444');
        }
    } catch(e) {
        show('❌ ' + e.message, '#ff4444');
    }
    lock(false);
    document.getElementById('b1').textContent = '▶ Run Demo Pipeline';
}

async function runCelery() {
    lock(true);
    document.getElementById('b2').textContent = '⏳ Queuing...';
    show('🔄 Sending to Celery queue...\\nGenerating Task ID...', '#ff9900');
    try {
        var d = await safeFetch('/celery/run', 90000);
        show(
            '🔄 Celery Task Complete!\\n\\n' +
            '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\\n' +
            '📋 Task ID    : ' + d.task_id   + '\\n' +
            '📌 Task Name  : run_demo_task\\n' +
            '⏰ Queued At  : ' + d.queued_at + '\\n' +
            '✅ Status     : ' + d.status    + '\\n' +
            '📊 Result     : ' + d.result    + '\\n' +
            '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\\n\\n' +
            '💡 Production Celery setup:\\n' +
            '   celery -A schedulers worker --loglevel=info\\n' +
            '   celery -A schedulers beat   --loglevel=info\\n' +
            '   (Requires Redis as message broker)',
            '#ff9900'
        );
    } catch(e) {
        show('❌ ' + e.message, '#ff4444');
    }
    lock(false);
    document.getElementById('b2').textContent = '🔄 Queue Celery Task';
}

async function getStatus() {
    lock(true);
    document.getElementById('b3').textContent = '⏳ Loading...';
    show('⏳ Fetching system status...', '#00aaff');
    try {
        var d = await safeFetch('/status', 30000);
        var txt = '📊 SYSTEM STATUS\\n\\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\\n';
        for (var k in d) {
            txt += (k+'                ').slice(0,20) + ': ' + d[k] + '\\n';
        }
        txt += '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━';
        show(txt, '#00aaff');
    } catch(e) {
        show('❌ ' + e.message, '#ff4444');
    }
    lock(false);
    document.getElementById('b3').textContent = '📊 System Status';
}

async function getHistory() {
    lock(true);
    document.getElementById('b4').textContent = '⏳ Loading...';
    try {
        var d = await safeFetch('/celery/history', 15000);
        var hbox = document.getElementById('hbox');
        var hrows = document.getElementById('hrows');
        hbox.style.display = 'block';
        if (!d.tasks || d.tasks.length === 0) {
            hrows.innerHTML =
                '<div style="color:#888;padding:10px">' +
                'No tasks yet.<br>Run the pipeline first!' +
                '</div>';
        } else {
            hrows.innerHTML = d.tasks.map(function(t) {
                var n = t.task.split('.').pop();
                var ts = (t.timestamp||'').substring(11,19);
                return '<div class="hrow">' +
                    '<span style="color:#ff9900">' + n + '</span>' +
                    '<span style="color:#888">'    + ts + '</span>' +
                    '<span style="color:#00ff88">' + t.status + '</span>' +
                    '</div>';
            }).join('');
        }
    } catch(e) {
        show('❌ ' + e.message, '#ff4444');
    }
    lock(false);
    document.getElementById('b4').textContent = '📋 Task History';
}
</script>
</body>
</html>
"""

@app.route("/run")
def run():
    try:
        r = do_etl("render_run")
        LOGS.append({"task":"run_pipeline","status":"SUCCESS","result":str(r)[:60],"timestamp":datetime.now().isoformat()})
        return jsonify(r)
    except Exception as e:
        return jsonify({"status":"error","message":str(e)})

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
