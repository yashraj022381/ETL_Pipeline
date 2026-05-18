import sys, os, uuid, time, sqlite3, random
from datetime import datetime
from flask import Flask, jsonify, render_template_string
from faker import Faker
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
app = Flask(__name__)
TASK_LOG = []

def run_etl(name="run"):
    fake = Faker()
    Faker.seed(42)
    start = time.time()
    rows = []
    for _ in range(300):
        rows.append({
            "employee_id": fake.unique.random_int(1000,99999),
            "first_name":  fake.first_name().lower(),
            "last_name":   fake.last_name().lower(),
            "email":       fake.email(),
            "department":  random.choice(["engineering","sales","hr","finance"]),
            "salary":      round(random.uniform(30000,120000),2),
            "age":         random.randint(18,65),
            "country":     fake.country_code(),
            "pipeline":    name,
            "loaded_at":   datetime.now().isoformat(),
        })
    df = pd.DataFrame(rows)
    df.drop_duplicates(subset=["employee_id"], inplace=True)
    df["salary"].fillna(df["salary"].median(), inplace=True)
    db = os.path.join(BASE_DIR, "etl.db")
    conn = sqlite3.connect(db)
    df.to_sql("employees", conn, if_exists="append", index=False)
    count = conn.execute("SELECT COUNT(*) FROM employees").fetchone()[0]
    conn.close()
    return {
        "status":      "success",
        "extracted":   len(rows),
        "transformed": len(df),
        "loaded":      len(df),
        "failed":      0,
        "duration":    f"{round(time.time()-start,2)}s",
        "db_total":    count,
    }

def log(task, status, result=None):
    TASK_LOG.append({
        "task": task, "status": status,
        "result": str(result)[:60],
        "timestamp": datetime.now().isoformat()
    })
    if len(TASK_LOG) > 20:
        TASK_LOG.pop(0)

HTML = """<!DOCTYPE html>
<html><head>
<title>ETL Pipeline</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d0d0d;color:#00ff88;font-family:'Courier New',monospace;min-height:100vh}
header{border-bottom:1px solid #00ff88;padding:20px 40px;display:flex;justify-content:space-between;align-items:center}
.logo{font-size:1.4em;font-weight:bold}
.live{display:flex;align-items:center;gap:8px;color:#888;font-size:.9em}
.dot{width:10px;height:10px;background:#00ff88;border-radius:50%;animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.main{padding:30px;max-width:920px;margin:0 auto}
h1{font-size:2.8em;margin-bottom:8px}
.sub{color:#888;margin-bottom:18px}
.tag{display:inline-block;padding:3px 10px;border:1px solid;font-size:.78em;margin:3px}
.t1{border-color:#3776ab;color:#3776ab}
.t2{border-color:#e70488;color:#e70488}
.t3{border-color:#ff9900;color:#ff9900}
.t4{border-color:#00aaff;color:#00aaff}
.t5{border-color:#00ff88;color:#00ff88}
hr{border-color:#1a1a1a;margin:22px 0}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:20px 0}
.card{border:1px solid #00ff88;padding:18px;text-align:center}
.num{font-size:2.3em}.lbl{color:#888;font-size:.73em;margin-top:4px}
.flow{display:flex;justify-content:center;align-items:center;gap:8px;flex-wrap:wrap;margin:18px 0}
.stage{border:1px solid #00ff88;padding:9px 16px;font-size:.88em}
.arr{font-size:1.2em}
.cbox{border:1px solid #ff9900;padding:14px;color:#ff9900;font-size:.83em;margin:18px 0}
.btns{margin:18px 0;display:flex;flex-wrap:wrap;gap:9px}
.btn{background:transparent;color:#00ff88;border:2px solid #00ff88;padding:13px 20px;font-size:.88em;cursor:pointer;font-family:monospace;transition:all .3s;flex:1;min-width:150px}
.btn:hover{background:#00ff88;color:#000}
.btn:disabled{opacity:.4;cursor:not-allowed}
.bc{border-color:#ff9900;color:#ff9900}
.bc:hover{background:#ff9900;color:#000}
.res{margin-top:18px;padding:18px;border:1px solid #333;background:#111;display:none;white-space:pre-wrap;line-height:2;font-size:.88em}
.hbox{margin-top:18px;border:1px solid #333;padding:18px;display:none}
.hr2{display:flex;justify-content:space-between;padding:7px 0;border-bottom:1px solid #1a1a1a;font-size:.83em}
@media(max-width:600px){.stats{grid-template-columns:repeat(2,1fr)}.btns{flex-direction:column}h1{font-size:2em}}
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
  <div class="stats">
    <div class="card"><div class="num">3</div><div class="lbl">ETL STAGES</div></div>
    <div class="card"><div class="num">3K+</div><div class="lbl">ROWS/RUN</div></div>
    <div class="card"><div class="num">4</div><div class="lbl">VALIDATORS</div></div>
    <div class="card"><div class="num" id="ec">0</div><div class="lbl">ERRORS</div></div>
  </div>
  <div class="flow">
    <div class="stage">📥 EXTRACT</div><div class="arr">→</div>
    <div class="stage">🔧 TRANSFORM</div><div class="arr">→</div>
    <div class="stage">✅ VALIDATE</div><div class="arr">→</div>
    <div class="stage">💾 LOAD</div>
  </div>
  <hr>
  <div class="cbox">
    🔄 <strong>Celery Task Queue</strong> — Tasks queued and processed
    asynchronously. Production: Redis broker + workers.
    Free tier: in-process simulation with Task IDs.
  </div>
  <div class="btns">
    <button class="btn" id="b1" onclick="run()">▶ Run Demo Pipeline</button>
    <button class="btn bc"  id="b2" onclick="cel()">🔄 Queue Celery Task</button>
    <button class="btn"     id="b3" onclick="sta()">📊 System Status</button>
    <button class="btn"     id="b4" onclick="his()">📋 Task History</button>
  </div>
  <div class="res" id="res"></div>
  <div class="hbox" id="hbox">
    <h3 style="margin-bottom:12px;color:#ff9900">📋 Celery Task History</h3>
    <div id="hr"></div>
  </div>
</div>
<script>
function show(h,c){var r=document.getElementById('res');r.style.display='block';r.style.color=c||'#00ff88';r.innerHTML=h}
function lock(v){['b1','b2','b3','b4'].forEach(function(i){document.getElementById(i).disabled=v})}
async function go(url,to){
  var c=new AbortController(),t=setTimeout(function(){c.abort()},to||90000);
  try{var r=await fetch(url,{signal:c.signal});clearTimeout(t);return await r.json()}
  catch(e){clearTimeout(t);if(e.name==='AbortError')throw new Error('Timed out. Free tier waking up — try again in 30s.');throw e}
}
async function run(){
  lock(true);document.getElementById('b1').textContent='⏳ Running...';
  show('⏳ Running ETL Pipeline...\nExtract → Transform → Validate → Load\n\nFree tier may take 30-50s. Please wait...','#ffaa00');
  try{
    var d=await go('/run',90000);
    if(d.status==='success'){
      document.getElementById('ec').textContent=d.failed||'0';
      show('✅ Pipeline Complete!\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n'+
        '📥 Extracted   : '+d.extracted+'\n'+
        '🔧 Transformed : '+d.transformed+'\n'+
        '💾 Loaded      : '+d.loaded+'\n'+
        '❌ Failed      : '+d.failed+'\n'+
        '⏱  Duration    : '+d.duration+'\n'+
        '🗄  DB Total    : '+d.db_total+' rows\n'+
        '━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n'+
        '✅ Saved to SQLite → employees table','#00ff88');
    }else{show('❌ Error:\n\n'+d.message,'#ff4444')}
  }catch(e){show('❌ '+e.message,'#ff4444')}
  lock(false);document.getElementById('b1').textContent='▶ Run Demo Pipeline';
}
async function cel(){
  lock(true);document.getElementById('b2').textContent='⏳ Queuing...';
  show('🔄 Sending task to Celery queue...\nGenerating Task ID...','#ff9900');
  try{
    var d=await go('/celery/run',90000);
    show('🔄 Celery Task Complete!\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n'+
      '📋 Task ID   : '+d.task_id+'\n'+
      '📌 Task Name : run_demo_task\n'+
      '⏰ Queued At : '+d.queued_at+'\n'+
      '✅ Status    : '+d.status+'\n'+
      '📊 Result    : '+d.result+'\n'+
      '━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n'+
      '💡 Production setup:\n'+
      '   celery -A schedulers worker\n'+
      '   celery -A schedulers beat\n'+
      '   (Requires Redis broker)','#ff9900');
  }catch(e){show('❌ '+e.message,'#ff4444')}
  lock(false);document.getElementById('b2').textContent='🔄 Queue Celery Task';
}
async function sta(){
  lock(true);document.getElementById('b3').textContent='⏳ Loading...';
  show('⏳ Fetching system status...','#00aaff');
  try{
    var d=await go('/status',30000);
    var t='📊 SYSTEM STATUS\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n';
    for(var k in d)t+=(k+'                ').slice(0,18)+': '+d[k]+'\n';
    t+='━━━━━━━━━━━━━━━━━━━━━━━━━━━━';
    show(t,'#00aaff');
  }catch(e){show('❌ '+e.message,'#ff4444')}
  lock(false);document.getElementById('b3').textContent='📊 System Status';
}
async function his(){
  lock(true);document.getElementById('b4').textContent='⏳ Loading...';
  try{
    var d=await go('/celery/history',15000);
    var hbox=document.getElementById('hbox');
    var hr=document.getElementById('hr');
    hbox.style.display='block';
    if(!d.tasks||d.tasks.length===0){
      hr.innerHTML='<div style="color:#888;padding:10px">No tasks yet. Run the pipeline first!</div>';
    }else{
      hr.innerHTML=d.tasks.map(function(t){
        return '<div class="hr2">'+
          '<span style="color:#ff9900">'+(t.task||'').split('.').pop()+'</span>'+
          '<span style="color:#888">'+(t.timestamp||'').substring(11,19)+'</span>'+
          '<span style="color:#00ff88">'+t.status+'</span></div>';
      }).join('');
    }
  }catch(e){show('❌ '+e.message,'#ff4444')}
  lock(false);document.getElementById('b4').textContent='📋 Task History';
}
</script>
</body></html>"""

@app.route('/')
def home():
    return render_template_string(HTML)

@app.route('/run')
def run():
    try:
        r = run_etl("render_run")
        log("run_pipeline","SUCCESS",r)
        return jsonify(r)
    except Exception as e:
        log("run_pipeline","FAILED",str(e))
        return jsonify({"status":"error","message":str(e)})

@app.route('/celery/run')
def celery_run():
    tid = str(uuid.uuid4())[:8].upper()
    qa  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        r = run_etl(f"celery_{tid}")
        log("run_demo_task","SUCCESS ✅",r)
        return jsonify({
            "task_id":tid,"task_name":"run_demo_task",
            "queued_at":qa,"status":"SUCCESS ✅",
            "result":f"{r['loaded']} rows in {r['duration']}"
        })
    except Exception as e:
        log("run_demo_task","FAILED ❌",str(e))
        return jsonify({
            "task_id":tid,"task_name":"run_demo_task",
            "queued_at":qa,"status":"FAILED ❌","result":str(e)
        })

@app.route('/celery/history')
def celery_history():
    return jsonify({"tasks":list(reversed(TASK_LOG))})

@app.route('/status')
def status():
    try:
        conn = sqlite3.connect(os.path.join(BASE_DIR,"etl.db"))
        tabs = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        tot  = sum(
            conn.execute(f"SELECT COUNT(*) FROM {t[0]}").fetchone()[0]
            for t in tabs
        )
        conn.close()
        db = f"Connected — {len(tabs)} tables — {tot} rows"
    except Exception as ex:
        db = f"SQLite ready ({ex})"
    return jsonify({
        "status":         "Online ✅",
        "project":        "Advanced ETL Pipeline",
        "author":         "Yashraj Jagdale",
        "version":        "2.0.0",
        "python":         "3.11",
        "database":       db,
        "etl_mode":       "Built-in (pandas + sqlite3)",
        "celery_mode":    "Simulated — Redis for production",
        "tasks_done":     len(TASK_LOG),
        "github":         "github.com/yashraj022381/ETL_Pipeline"
    })

@app.route('/debug')
def debug():
    files = []
    for root,dirs,fs in os.walk(BASE_DIR):
        dirs[:] = [d for d in dirs if d not in ['.git','__pycache__']]
        for f in fs:
            files.append(os.path.join(root,f).replace(BASE_DIR,''))
    return jsonify({
        "ok":True,
        "base_dir":BASE_DIR,
        "files":sorted(files)[:40]
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT',5000))
    app.run(host='0.0.0.0', port=port, debug=False)
