import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>ETL Pipeline</title>
    <style>
        body {
            background: #0a0a0a;
            color: #00ff88;
            font-family: monospace;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            margin: 0;
        }
        .box {
            border: 2px solid #00ff88;
            padding: 40px;
            text-align: center;
            max-width: 600px;
        }
        h1 { font-size: 2em; }
        .btn {
            background: #00ff88;
            color: #000;
            border: none;
            padding: 12px 30px;
            font-size: 1em;
            cursor: pointer;
            margin: 10px;
            font-family: monospace;
            font-weight: bold;
        }
        .result {
            margin-top: 20px;
            padding: 15px;
            border: 1px solid #00ff88;
            display: none;
        }
    </style>
</head>
<body>
<div class="box">
    <h1>⚡ ETL Pipeline</h1>
    <p>Extract → Transform → Load</p>
    <p>Built with Python + Pandas + SQLite</p>
    <hr style="border-color:#00ff88">
    <button class="btn" onclick="runPipeline()">▶ Run Demo Pipeline</button>
    <button class="btn" onclick="getStatus()">📊 Get Status</button>
    <div class="result" id="result"></div>
</div>
<script>
    async function runPipeline() {
        document.getElementById('result').style.display = 'block';
        document.getElementById('result').innerHTML = '⏳ Running pipeline...';
        const r = await fetch('/run');
        const d = await r.json();
        document.getElementById('result').innerHTML =
            '✅ Done!<br>' +
            'Rows Loaded: ' + d.loaded + '<br>' +
            'Failed: ' + d.failed + '<br>' +
            'Duration: ' + d.duration;
    }
    async function getStatus() {
        document.getElementById('result').style.display = 'block';
        const r = await fetch('/status');
        const d = await r.json();
        document.getElementById('result').innerHTML =
            JSON.stringify(d, null, 2).replace(/\n/g,'<br>');
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
        pipeline = ETLPipeline("web_run")
        result = pipeline.run(source="demo", target_table="employees")
        result["duration"] = f"{round(time.time()-start, 2)}s"
        result["status"] = "success"
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route('/status')
def status():
    return jsonify({
        "status": "running",
        "project": "Advanced ETL Pipeline",
        "version": "2.0.0",
        "endpoints": ["/", "/run", "/status"]
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
