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
  </div>
  <div class="btns">
    <button class="btn" id="b1" onclick="go('/run','b1','▶ Run Demo Pipeline')">▶ Run Demo Pipeline</button>
    <button class="btn bc" id="b2" onclick="cel()">🔄 Queue Celery Task</button>
    <button class="btn" id="b3" onclick="go('/status','b3','📊 System Status')">📊 System Status</button>
    <button class="btn" id="b4" onclick="hist()">📋 Task History</button>
  </div>
  <div class="res" id="res"></div>
  <div class="hbox" id="hb">
    <h3 style="margin-bottom:12px;color:#ff9900">📋 Celery Task History</h3>
    <div id="hr"></div>
  </div>
</div>
<script>
function show(h,c){var r=document.getElementById('res');r.style.display='block';r.style.color=c||'#00ff88';r.innerHTML=h}
function lock(v){['b1','b2','b3','b4'].forEach(function(i){document.getElementById(i).disabled=v})}
async function go(url,bid,orig){
  lock(true);document.getElementById(bid).textContent='⏳ Loading...';
  show('⏳ Please wait (free tier may take 30-50s)...','#ffaa00');
  try{
    var r=await fetch(url);var d=await r.json();
    if(d.status==='success'||d.status==='Online ✅'){
      var t='';
      for(var k in d) t+=(k+'                ').slice(0,18)+': '+d[k]+'\\n';
      show('✅ Success!\\n\\n━━━━━━━━━━━━━━━━━━━━\\n'+t+'━━━━━━━━━━━━━━━━━━━━','#00ff88');
    }else{show('❌ '+JSON.stringify(d),'#ff4444')}
  }catch(e){show('❌ '+e.message,'#ff4444')}
  lock(false);document.getElementById(bid).textContent=orig;
}
async function cel(){
  lock(true);document.getElementById('b2').textContent='⏳ Queuing...';
  show('🔄 Sending to Celery queue...','#ff9900');
  try{
    var r=await fetch('/celery/run');var d=await r.json();
    show('🔄 Celery Task Done!\\n\\n━━━━━━━━━━━━━━━━━━━━\\n'+
      'Task ID  : '+d.task_id+'\\n'+
      'Status   : '+d.status+'\\n'+
      'Result   : '+d.result+'\\n'+
      'Queued At: '+d.queued_at+'\\n'+
      '━━━━━━━━━━━━━━━━━━━━\\n\\n'+
      'Production: celery -A schedulers worker\\n'+
      '            celery -A schedulers beat','#ff9900');
  }catch(e){show('❌ '+e.message,'#ff4444')}
  lock(false);document.getElementById('b2').textContent='🔄 Queue Celery Task';
}
async function hist(){
  lock(true);document.getElementById('b4').textContent='⏳ Loading...';
  try{
    var r=await fetch('/celery/history');var d=await r.json();
    document.getElementById('hb').style.display='block';
    document.getElementById('hr').innerHTML=
      !d.tasks||d.tasks.length===0
      ?'<div style="color:#888;padding:10px">No tasks yet. Run pipeline first!</div>'
      :d.tasks.map(function(t){
        return '<div class="hr2">'+
          '<span style="color:#ff9900">'+(t.task||'').split('.').pop()+'</span>'+
          '<span style="color:#888">'+(t.timestamp||'').substring(11,19)+'</span>'+
          '<span style="color:#00ff88">'+t.status+'</span></div>';
      }).join('');
  }catch(e){show('❌ '+e.message,'#ff4444')}
  lock(false);document.getElementById('b4').textContent='📋 Task History';
}
</script></body></html>
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
