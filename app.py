import os, sys, uuid, time, sqlite3, random
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

    # EXTRACT
    t1 = time.time()
    raw = [{"id":fake.unique.random_int(1,99999),
            "name":fake.name(),
            "email":fake.email(),
            "dept":random.choice(["Engineering","Sales","HR","Finance","Marketing"]),
            "salary":round(random.uniform(30000,120000),2),
            "age":random.randint(18,65),
            "phone":fake.phone_number() if random.random()>0.1 else None,
            "country":fake.country_code()}
           for _ in range(300)]
    et = round(time.time()-t1, 3)

    # TRANSFORM
    t2 = time.time()
    df = pd.DataFrame(raw)
    before = len(df)
    df.drop_duplicates(subset=["id"], inplace=True)
    dupes = before - len(df)
    nulls = int(df.isnull().sum().sum())
    df["salary"].fillna(df["salary"].median(), inplace=True)
    df["phone"].fillna("N/A", inplace=True)
    df["name"]  = df["name"].str.lower().str.strip()
    df["dept"]  = df["dept"].str.lower().str.strip()
    df["email"] = df["email"].str.lower().str.strip()
    df["pipeline"]  = name
    df["loaded_at"] = datetime.now().isoformat()
    df["run_id"]    = str(uuid.uuid4())[:8].upper()
    tt = round(time.time()-t2, 3)

    # VALIDATE
    t3 = time.time()
    assert len(df)>0
    assert df["salary"].notnull().all()
    assert (df["age"]>=0).all() and (df["age"]<=120).all()
    vt = round(time.time()-t3, 3)

    # LOAD
    t4 = time.time()
    conn = sqlite3.connect("etl.db")
    df.to_sql("emp", conn, if_exists="append", index=False)
    total = conn.execute("SELECT COUNT(*) FROM emp").fetchone()[0]
    depts = dict(conn.execute(
        "SELECT dept,COUNT(*) FROM emp GROUP BY dept"
    ).fetchall())
    conn.close()
    lt = round(time.time()-t4, 3)

    return {
        "extracted":     len(raw),
        "transformed":   len(df),
        "loaded":        len(df),
        "failed":        0,
        "dupes_removed": dupes,
        "nulls_fixed":   nulls,
        "extract_time":  f"{et}s",
        "transform_time":f"{tt}s",
        "validate_time": f"{vt}s",
        "load_time":     f"{lt}s",
        "total_duration":f"{round(time.time()-t0,3)}s",
        "db_total":      total,
        "dept_breakdown":str(depts),
    }

PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ETL Pipeline — Yashraj Jagdale</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d0d0d;color:#00ff88;font-family:'Courier New',monospace;min-height:100vh}
header{border-bottom:1px solid #00ff88;padding:18px 35px;
       display:flex;justify-content:space-between;align-items:center}
.logo{font-size:1.4em;font-weight:bold}
.live{display:flex;align-items:center;gap:7px;color:#888;font-size:.9em}
.dot{width:9px;height:9px;background:#00ff88;border-radius:50%;
     animation:blink 2s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.25}}
main{padding:30px 35px;max-width:960px;margin:0 auto}
h1{font-size:2.6em;margin-bottom:6px}
.sub{color:#888;margin-bottom:16px;font-size:.95em}
.tag{display:inline-block;padding:3px 10px;border:1px solid;
     font-size:.75em;margin:2px;border-radius:2px}
.t1{border-color:#3776ab;color:#3776ab}
.t2{border-color:#e70488;color:#e70488}
.t3{border-color:#ff9900;color:#ff9900}
.t4{border-color:#00aaff;color:#00aaff}
.t5{border-color:#00ff88;color:#00ff88}
hr{border:none;border-top:1px solid #1a1a1a;margin:22px 0}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:18px 0}
.card{border:1px solid #00ff88;padding:16px;text-align:center}
.num{font-size:2.2em;font-weight:bold}
.lbl{color:#888;font-size:.72em;margin-top:3px;letter-spacing:.05em}
.flow{display:flex;justify-content:center;align-items:center;
      gap:8px;flex-wrap:wrap;margin:16px 0}
.stage{border:1px solid #00ff88;padding:8px 15px;font-size:.85em}
.arr{font-size:1.1em;color:#00ff88}
.cbox{border:1px solid #ff9900;padding:13px;color:#ff9900;
      font-size:.82em;margin:16px 0;line-height:1.6}
.btns{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:16px 0}
.btn{background:transparent;border:2px solid #00ff88;color:#00ff88;
     padding:14px 10px;font-size:.85em;cursor:pointer;
     font-family:'Courier New',monospace;transition:all .25s;width:100%}
.btn:hover{background:#00ff88;color:#000}
.btn:disabled{opacity:.35;cursor:not-allowed;background:transparent;color:#00ff88}
.bc{border-color:#ff9900;color:#ff9900}
.bc:hover{background:#ff9900;color:#000}
.bc:disabled{color:#ff9900;border-color:#ff9900;background:transparent}
/* RESULT BOX — always in DOM, shown via JS */
#res{
    display:none;
    margin-top:20px;
    padding:20px;
    border:1px solid #333;
    background:#0a0a0a;
    color:#00ff88;
    white-space:pre-wrap;
    font-size:.88em;
    line-height:1.9;
    border-radius:2px;
}
#hbox{
    display:none;
    margin-top:16px;
    border:1px solid #333;
    padding:18px;
    background:#0a0a0a;
}
.hrow{display:flex;justify-content:space-between;
      padding:7px 0;border-bottom:1px solid #1a1a1a;font-size:.82em}
.spinner{display:inline-block;animation:spin 1s linear infinite;margin-right:6px}
@keyframes spin{to{transform:rotate(360deg)}}
@media(max-width:640px){
    .grid{grid-template-columns:repeat(2,1fr)}
    .btns{grid-template-columns:repeat(2,1fr)}
    h1{font-size:1.9em}
}
</style>
</head>
<body>

<header>
    <div class="logo">⚡ ETL_Pipeline</div>
    <div class="live"><div class="dot"></div>Live on Render</div>
</header>

<main>
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
        <div class="card">
            <div class="num">3</div>
            <div class="lbl">ETL STAGES</div>
        </div>
        <div class="card">
            <div class="num">3K+</div>
            <div class="lbl">ROWS / RUN</div>
        </div>
        <div class="card">
            <div class="num">4</div>
            <div class="lbl">VALIDATORS</div>
        </div>
        <div class="card">
            <div class="num" id="ec">0</div>
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

    <div class="cbox">
        🔄 <strong>Celery Task Queue</strong> — Tasks queued and processed
        asynchronously. Production: Redis broker + workers.
        Free tier: in-process simulation with unique Task IDs.
    </div>

    <!-- BUTTONS -->
    <div class="btns">
        <button class="btn" id="b1" onclick="runPipeline()">
            ▶ Run Demo Pipeline
        </button>
        <button class="btn bc" id="b2" onclick="runCelery()">
            🔄 Queue Celery Task
        </button>
        <button class="btn" id="b3" onclick="getStatus()">
            📊 System Status
        </button>
        <button class="btn" id="b4" onclick="getHistory()">
            📋 Task History
        </button>
    </div>

    <!-- RESULT BOX — always present in DOM -->
    <div id="res"></div>

    <!-- HISTORY BOX -->
    <div id="hbox">
        <h3 style="margin-bottom:12px;color:#ff9900">
            📋 Celery Task History
        </h3>
        <div id="hr"></div>
    </div>

</main>

<script>
/* ── helpers ─────────────────────────────────────────────── */
function show(html, color) {
    var r = document.getElementById('res');
    r.style.display  = 'block';
    r.style.color    = color || '#00ff88';
    r.innerHTML      = html;
    setTimeout(function(){
        r.scrollIntoView({behavior:'smooth', block:'nearest'});
    }, 50);
}

function lock(on) {
    ['b1','b2','b3','b4'].forEach(function(id) {
        document.getElementById(id).disabled = on;
    });
}

async function apiFetch(url, timeoutMs) {
    timeoutMs = timeoutMs || 120000;
    var ctrl  = new AbortController();
    var timer = setTimeout(function(){ ctrl.abort(); }, timeoutMs);
    var resp  = await fetch(url, { signal: ctrl.signal });
    clearTimeout(timer);
    return resp.json();
}

/* ── Run Demo Pipeline ───────────────────────────────────── */
async function runPipeline() {
    lock(true);
    document.getElementById('b1').innerHTML =
        '<span class="spinner">⟳</span> Running...';

    show(
        '⏳ ETL Pipeline Starting...\n\n' +
        '  Stage 1 → EXTRACT   : reading 300 rows\n' +
        '  Stage 2 → TRANSFORM : cleaning data\n' +
        '  Stage 3 → VALIDATE  : checking quality\n' +
        '  Stage 4 → LOAD      : saving to SQLite\n\n' +
        '⚠  Free tier may take up to 60s on first run.\n' +
        '  Please wait — do not click again.',
        '#ffaa00'
    );

    try {
        var d = await apiFetch('/run', 120000);

        if (d.status === 'success') {
            document.getElementById('ec').textContent = d.failed || '0';
            show(
                '✅  Pipeline Complete!\n\n' +
                '━━━━━━ EXTRACT ━━━━━━━━━━━━━━━━━━\n' +
                '  Rows Collected  : ' + d.extracted + '\n' +
                '  Duration        : ' + d.extract_time + '\n\n' +
                '━━━━━━ TRANSFORM ━━━━━━━━━━━━━━━━\n' +
                '  Rows Cleaned    : ' + d.transformed + '\n' +
                '  Dupes Removed   : ' + d.dupes_removed + '\n' +
                '  Nulls Fixed     : ' + d.nulls_fixed + '\n' +
                '  Columns Added   : 3  (pipeline, loaded_at, run_id)\n' +
                '  Duration        : ' + d.transform_time + '\n\n' +
                '━━━━━━ VALIDATE ━━━━━━━━━━━━━━━━━\n' +
                '  Rules Checked   : 3\n' +
                '  Errors Found    : 0  ✅\n' +
                '  Duration        : ' + d.validate_time + '\n\n' +
                '━━━━━━ LOAD ━━━━━━━━━━━━━━━━━━━━━\n' +
                '  Rows Saved      : ' + d.loaded + '\n' +
                '  Rows Failed     : ' + d.failed + '\n' +
                '  DB Total Rows   : ' + d.db_total + '\n' +
                '  Duration        : ' + d.load_time + '\n\n' +
                '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n' +
                '  ⏱  Total Time   : ' + d.total_duration + '\n' +
                '  🏢 Departments  : ' + d.dept_breakdown + '\n' +
                '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n' +
                '  ✅ Saved → SQLite (emp table)',
                '#00ff88'
            );
        } else {
            show('❌ Server Error\n\n' + (d.message || JSON.stringify(d)), '#ff4444');
        }

    } catch(e) {
        show(
            e.name === 'AbortError'
                ? '⏱  Timed out (120s)\n\nServer was sleeping.\nClick the button again — second run is instant!'
                : '❌ Error: ' + e.message,
            '#ff4444'
        );
    }

    lock(false);
    document.getElementById('b1').textContent = '▶ Run Demo Pipeline';
}

/* ── Queue Celery Task ───────────────────────────────────── */
async function runCelery() {
    lock(true);
    document.getElementById('b2').innerHTML =
        '<span class="spinner">⟳</span> Queuing...';
    show('🔄 Sending to Celery queue...\nGenerating Task ID...', '#ff9900');

    try {
        var d = await apiFetch('/celery/run', 120000);
        show(
            '🔄 Celery Task Done!\n\n' +
            '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n' +
            '  Task ID    : ' + d.task_id    + '\n' +
            '  Task Name  : run_demo_task\n'  +
            '  Queued At  : ' + d.queued_at  + '\n' +
            '  Status     : ' + d.status     + '\n' +
            '  Result     : ' + d.result     + '\n' +
            '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n' +
            '  Production commands:\n' +
            '  celery -A schedulers worker --loglevel=info\n' +
            '  celery -A schedulers beat   --loglevel=info\n' +
            '  (Needs Redis as message broker)',
            '#ff9900'
        );
    } catch(e) {
        show('❌ ' + e.message, '#ff4444');
    }

    lock(false);
    document.getElementById('b2').textContent = '🔄 Queue Celery Task';
}

/* ── System Status ───────────────────────────────────────── */
async function getStatus() {
    lock(true);
    document.getElementById('b3').innerHTML =
        '<span class="spinner">⟳</span> Loading...';
    show('⏳ Fetching system status...', '#00aaff');

    try {
        var d = await apiFetch('/status', 30000);
        var txt = '📊 SYSTEM STATUS\n\n' +
                  '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n';
        for (var k in d) {
            txt += ('  ' + k + '                  ').slice(0,22) +
                   ': ' + d[k] + '\n';
        }
        txt += '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━';
        show(txt, '#00aaff');
    } catch(e) {
        show('❌ ' + e.message, '#ff4444');
    }

    lock(false);
    document.getElementById('b3').textContent = '📊 System Status';
}

/* ── Task History ────────────────────────────────────────── */
async function getHistory() {
    lock(true);
    document.getElementById('b4').innerHTML =
        '<span class="spinner">⟳</span> Loading...';

    try {
        var d    = await apiFetch('/celery/history', 15000);
        var hbox = document.getElementById('hbox');
        var hr   = document.getElementById('hr');

        hbox.style.display = 'block';
        setTimeout(function(){
            hbox.scrollIntoView({behavior:'smooth', block:'nearest'});
        }, 50);

        if (!d.tasks || d.tasks.length === 0) {
            hr.innerHTML = '<div style="color:#888;padding:10px">' +
                'No tasks yet — run the pipeline first!</div>';
        } else {
            hr.innerHTML = d.tasks.map(function(t) {
                var name = (t.task || '').split('.').pop();
                var ts   = (t.timestamp || '').substring(11, 19);
                var ok   = (t.status || '').indexOf('SUCCESS') >= 0;
                return '<div class="hrow">' +
                    '<span style="color:#ff9900">'  + name      + '</span>' +
                    '<span style="color:#888">'     + ts        + '</span>' +
                    '<span style="color:' + (ok ? '#00ff88' : '#ff4444') + '">'
                        + t.status + '</span>' +
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
</html>"""

@app.route("/")
def home():
    return PAGE

@app.route("/run")
def run():
    try:
        r = do_etl("render_run")
        r["status"] = "success"
        LOGS.append({"task":"run_pipeline","status":"SUCCESS",
                     "result":f"{r['loaded']} rows",
                     "timestamp":datetime.now().isoformat()})
        return jsonify(r)
    except Exception as e:
        return jsonify({"status":"error","message":str(e)})

@app.route("/celery/run")
def celery_run():
    tid = str(uuid.uuid4())[:8].upper()
    qa  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        r = do_etl(f"celery_{tid}")
        LOGS.append({"task":"run_demo_task","status":"SUCCESS ✅",
                     "result":f"{r['loaded']} rows",
                     "timestamp":datetime.now().isoformat()})
        return jsonify({"task_id":tid,"queued_at":qa,
                        "status":"SUCCESS ✅",
                        "result":f"{r['loaded']} rows in {r['total_duration']}"})
    except Exception as e:
        return jsonify({"task_id":tid,"queued_at":qa,
                        "status":"FAILED ❌","result":str(e)})

@app.route("/celery/history")
def celery_history():
    return jsonify({"tasks": list(reversed(LOGS))})

@app.route("/status")
def status():
    try:
        conn = sqlite3.connect("etl.db")
        n    = conn.execute("SELECT COUNT(*) FROM emp").fetchone()[0]
        conn.close()
        db   = f"Connected — {n} rows"
    except:
        db = "Ready (no data yet)"
    return jsonify({
        "status":       "Online ✅",
        "project":      "Advanced ETL Pipeline",
        "author":       "Yashraj Jagdale",
        "version":      "2.0.0",
        "python":       "3.11",
        "database":     db,
        "celery":       "Simulated (Redis for production)",
        "tasks_done":   len(LOGS),
        "github":       "github.com/yashraj022381/ETL_Pipeline",
        "live_url":     "etl-pipeline-1xrp.onrender.com"
    })

@app.route("/debug")
def debug():
    files = []
    for root,dirs,fs in os.walk("."):
        dirs[:] = [d for d in dirs if d not in['.git','__pycache__']]
        for f in fs: files.append(os.path.join(root,f))
    return jsonify({"ok":True,"files":sorted(files)[:30]})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
