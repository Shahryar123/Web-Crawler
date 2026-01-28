from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
from crawler import WebCrawler
import threading
import time

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Store active crawlers
active_crawlers = {}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start-crawl', methods=['POST'])
def start_crawl():
    data = request.json
    start_url = data.get('url')
    max_depth = int(data.get('max_depth', 3))
    max_urls = int(data.get('max_urls', 1000))
    
    if not start_url:
        return jsonify({'error': 'URL is required'}), 400
    
    # Create a unique session ID
    session_id = str(time.time())
    
    # Create crawler instance
    crawler = WebCrawler(
        start_url=start_url,
        max_depth=max_depth,
        max_urls=max_urls,
        socketio=socketio,
        session_id=session_id
    )
    
    active_crawlers[session_id] = crawler
    
    # Start crawling in a separate thread
    thread = threading.Thread(target=crawler.crawl)
    thread.daemon = True
    thread.start()
    
    return jsonify({'session_id': session_id, 'status': 'started'})

@app.route('/download-sitemap/<session_id>')
def download_sitemap(session_id):
    if session_id not in active_crawlers:
        return jsonify({'error': 'Session not found'}), 404
    
    crawler = active_crawlers[session_id]
    sitemap_xml = crawler.generate_sitemap()
    
    from flask import Response
    return Response(
        sitemap_xml,
        mimetype='application/xml',
        headers={'Content-Disposition': 'attachment;filename=sitemap.xml'}
    )

@app.route('/get-urls/<session_id>')
def get_urls(session_id):
    if session_id not in active_crawlers:
        return jsonify({'error': 'Session not found'}), 404
    
    crawler = active_crawlers[session_id]
    return jsonify({
        'urls': list(crawler.valid_urls),
        'total': len(crawler.valid_urls)
    })

if __name__ == '__main__':
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)