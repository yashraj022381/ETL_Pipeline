import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>ETL Pipeline — Yashraj</title>
    <style>
        * { margin:0; padding:0; box-sizing:border-box; }
        body {
            background: #0d0d0d;
            color: #00ff88;
            font-family: 'Courier New', monospace;
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .container {
            border: 2px solid #00ff88;
            padding: 50px;
            max-width: 700px;
            width: 90%;
            text-align: center;
        }
        h1 { font-size: 2.5em; margin-bottom: 10px; }
        .subtitle { color: #888; margin-bottom: 30px; font-size: 1.1em; }
        .tech { color: #00aaff; margin-bottom: 40px; }
        .btn {
            background: transparent;
            color: #00ff88;
            border: 2px solid #00ff88;
            padding: 15px 35px;
            font-size: 1em;
            cursor: pointer;
            margin: 8px;
            font-family: monospace;
            transition: all 0.3s;
        }
        .btn:hover { background: #00ff88; color: #000; }
        .result {
            margin-top: 30px;
            padding: 20px;
            border: 1px solid #333;
            text-align: left;
            display: none;
            background: #111;
        }
        .success { color: #00ff88; }
        .error { color: #ff4444; }
        .stat {
            display: inline-block;
            margin: 10px 20px;
            text-align: center;
        }
        .stat-num { font-size: 2em; color: #00ff88; }
        .stat-label { color: #888; font-size: 0.8em; }
        hr { border-color: #333; margin: 30px 0; }
    </style>
</head>
<body>
<div class="container">
    <h1>⚡ ETL Pipeline</h1>
    <p class="subtitle">Advanced Data Engineering Project</p>
    <p class="tech">Python + Pandas + SQLite + Flask</p>

    <hr>

    <div>
        <div class="stat">
            <div class="stat-num">3</div>
            <div class="stat-label">Pipeline Modes</div>
        </div>
        <div class="stat">
            <div class="stat-num">10K+</div>
            <div class="stat-label">Rows Processed</div>
        </div>
        <div class="stat">
            <div class="stat-num">4</div>
            <div class="stat-label">ETL Stages</div>
        </div>
    </div>

    <hr>

    <button class="btn" onclick="runPipeline()">▶ Run Pipeline</button>
    <button class="btn" onclick="getStatus()">📊 Status</button>

    <div class="result" id="result"></div>
</div>

<script>
async function runPipeline() {
    const r = document.getElementById('result');
    r.style.display = 'block';
    r.innerHTML = '<span style="color:#ffaa00">⏳ Running ETL Pipeline...</span>';
    try {
        const res = await fetch('/run');
        const d = await res.json();
        if (d.status === 'success') {
            r.innerHTML = '<span class="success">' +
                '✅ Pipeline Complete!<br><br>' +
                '📥 Rows Extracted : ' + d.extracted + '<br>' +
                '🔧 Rows Transformed: ' + d.transformed + '<br>' +
                '💾 Rows Loaded    : ' + d.loaded + '<br>' +
                '❌ Rows Failed    : ' + d.failed + '<br>' +
                '⏱  Duration       : ' + d.duration +
                '</span>';
        } else {
            r.innerHTML = '<span class="error">❌ Error: ' + d.message + '</span>';
        }
    } catch(e) {
        r.innerHTML = '<span class="error">❌ ' + e + '</span>';
    }
}

async function getStatus() {
    const r = document.getElementById('result');
    r.style.display = 'block';
    const res = await fetch('/status');
    const d = await res.json();
    r.innerHTML = '<span class="success">' +
        Object.entries(d).map(([k,v]) => k + ': ' + v).join('<br>') +
        '</span>';
}
</script>
</body>
</html>
"""

@app.route('/')
def home():
    return render_template_string(HTML)

@app.route('/run')
def run_pipeline():
    import time
    start = time.time()
    try:
        from pipeline import ETLPipeline
        pipeline = ETLPipeline("render_run")
        result = pipeline.run(
            source="demo",
            target_table="employees"
        )
        return jsonify({
            "status":      "success",
            "extracted":   result.get("loaded", 0),
            "transformed": result.get("loaded", 0),
            "loaded":      result.get("loaded", 0),
            "failed":      result.get("failed", 0),
            "duration":    f"{round(time.time()-start, 2)}s"
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        })

@app.route('/status')
def status():
    return jsonify({
        "status":    "✅ Online",
        "project":   "Advanced ETL Pipeline",
        "author":    "Yashraj Jagdale",
        "version":   "2.0.0",
        "tech":      "Python + Pandas + SQLite + Flask",
        "endpoints": "/  |  /run  |  /status"
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
