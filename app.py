import os
import json
import mimetypes
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import unquote, parse_qs, urlparse
import sqlite3
from datetime import datetime

# Configuration
PORT = int(os.getenv('PORT', 8000))

# IMPORTANT: Set your beats folder path here
# For Windows: r"D:\FL Projects\CNX2\Uploaded\Astro Blacksmith x 963 Beats - Premium Beatstore\Astro Blacksmith Vault\beats"
# For Mac/Linux: "/path/to/beats"
BEATS_FOLDER = os.getenv('BEATS_FOLDER', r"D:\FL Projects\CNX2\Uploaded\Astro Blacksmith x 963 Beats - Premium Beatstore\Astro Blacksmith Vault\beats")

DB_PATH = './beats.db'

# Ensure beats folder exists
try:
    Path(BEATS_FOLDER).mkdir(parents=True, exist_ok=True)
except Exception as e:
    print(f"Warning: Could not create beats folder: {e}")


class BeatsDatabase:
    """SQLite database for beat metadata and inquiries."""
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """Initialize database tables."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Beats table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS beats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    slug TEXT UNIQUE NOT NULL,
                    description TEXT,
                    genre TEXT,
                    bpm INTEGER,
                    duration INTEGER,
                    file_name TEXT NOT NULL,
                    file_type TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Exclusive inquiries table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS exclusive_inquiries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    beat_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    email TEXT NOT NULL,
                    offer TEXT NOT NULL,
                    status TEXT DEFAULT 'new',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(beat_id) REFERENCES beats(id)
                )
            ''')
            
            conn.commit()
    
    def get_all_beats(self):
        """Fetch all beats from database or scan folder."""
        # First, scan the beats folder for new files
        self.sync_beats_from_folder()
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM beats ORDER BY created_at DESC')
            return [dict(row) for row in cursor.fetchall()]
    
    def sync_beats_from_folder(self):
        """Scan beats folder and add new files to database."""
        if not os.path.exists(BEATS_FOLDER):
            print(f"Beats folder not found: {BEATS_FOLDER}")
            return
        
        try:
            # Get all audio files
            audio_extensions = ('.mp3', '.wav', '.m4a', '.flac', '.ogg')
            files = [f for f in os.listdir(BEATS_FOLDER) if f.lower().endswith(audio_extensions)]
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                for file_name in files:
                    # Check if file already in database
                    cursor.execute('SELECT id FROM beats WHERE file_name = ?', (file_name,))
                    if cursor.fetchone():
                        continue
                    
                    # Extract metadata from filename
                    title = os.path.splitext(file_name)[0]
                    file_type = os.path.splitext(file_name)[1][1:].lower()
                    slug = title.lower().replace(' ', '-').replace('_', '-')
                    
                    # Try to get duration (simplified - you can enhance this)
                    duration = None
                    
                    # Insert new beat
                    cursor.execute('''
                        INSERT INTO beats (title, slug, description, genre, bpm, duration, file_name, file_type)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (title, slug, None, None, None, duration, file_name, file_type))
                
                conn.commit()
                print(f"Synced {len(files)} beats from folder")
        except Exception as e:
            print(f"Error syncing beats: {e}")
    
    def get_beat_by_id(self, beat_id):
        """Fetch beat by ID."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM beats WHERE id = ?', (beat_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def add_beat(self, title, slug, description, genre, bpm, duration, file_name, file_type):
        """Add a new beat to database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO beats (title, slug, description, genre, bpm, duration, file_name, file_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (title, slug, description, genre, bpm, duration, file_name, file_type))
            conn.commit()
            return cursor.lastrowid
    
    def save_inquiry(self, beat_id, name, email, offer):
        """Save exclusive license inquiry."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO exclusive_inquiries (beat_id, name, email, offer)
                VALUES (?, ?, ?, ?)
            ''', (beat_id, name, email, offer))
            conn.commit()
            return cursor.lastrowid


class BeatStoreHandler(BaseHTTPRequestHandler):
    """HTTP request handler for beat store."""
    
    db = BeatsDatabase(DB_PATH)
    
    def do_GET(self):
        """Handle GET requests."""
        parsed_path = unquote(self.path)
        
        # API: Get all beats
        if parsed_path == '/api/beats':
            return self.handle_beats_list()
        
        # API: Get beat by ID
        if parsed_path.startswith('/api/beats/'):
            beat_id = parsed_path.split('/')[-1]
            return self.handle_get_beat(beat_id)
        
        # API: Stream audio file
        if parsed_path.startswith('/api/audio/'):
            file_name = unquote(parsed_path.replace('/api/audio/', ''))
            return self.handle_audio_stream(file_name)
        
        # Serve static files (index.html, etc.)
        if parsed_path == '/' or parsed_path == '/index.html':
            return self.serve_file('index.html', 'text/html')
        
        # 404
        self.send_error(404)
    
    def do_POST(self):
        """Handle POST requests."""
        parsed_path = unquote(self.path)
        
        # API: Submit exclusive inquiry
        if parsed_path == '/api/inquiry':
            return self.handle_inquiry_submission()
        
        self.send_error(404)
    
    def handle_beats_list(self):
        """Return list of all beats."""
        try:
            beats = self.db.get_all_beats()
            
            # Convert to JSON-serializable format
            beats_data = []
            for beat in beats:
                beat_dict = {
                    'id': beat['id'],
                    'title': beat['title'],
                    'slug': beat['slug'],
                    'description': beat['description'],
                    'genre': beat['genre'],
                    'bpm': beat['bpm'],
                    'duration': beat['duration'],
                    'fileType': beat['file_type'],
                    'fileName': beat['file_name'],
                    'fileUrl': f'/api/audio/{beat["file_name"]}'
                }
                beats_data.append(beat_dict)
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(beats_data).encode())
        except Exception as e:
            print(f"Error fetching beats: {e}")
            self.send_error(500)
    
    def handle_get_beat(self, beat_id):
        """Return specific beat by ID."""
        try:
            beat = self.db.get_beat_by_id(int(beat_id))
            
            if not beat:
                self.send_error(404)
                return
            
            beat_dict = {
                'id': beat['id'],
                'title': beat['title'],
                'slug': beat['slug'],
                'description': beat['description'],
                'genre': beat['genre'],
                'bpm': beat['bpm'],
                'duration': beat['duration'],
                'fileType': beat['file_type'],
                'fileName': beat['file_name'],
                'fileUrl': f'/api/audio/{beat["file_name"]}'
            }
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(beat_dict).encode())
        except Exception as e:
            print(f"Error fetching beat: {e}")
            self.send_error(500)
    
    def handle_audio_stream(self, file_name):
        """Stream audio file."""
        try:
            file_path = os.path.join(BEATS_FOLDER, file_name)
            
            # Security: prevent directory traversal
            if not os.path.abspath(file_path).startswith(os.path.abspath(BEATS_FOLDER)):
                self.send_error(403)
                return
            
            if not os.path.exists(file_path):
                print(f"File not found: {file_path}")
                self.send_error(404)
                return
            
            # Determine content type
            content_type, _ = mimetypes.guess_type(file_path)
            if not content_type:
                if file_name.lower().endswith('.mp3'):
                    content_type = 'audio/mpeg'
                elif file_name.lower().endswith('.wav'):
                    content_type = 'audio/wav'
                elif file_name.lower().endswith('.m4a'):
                    content_type = 'audio/mp4'
                else:
                    content_type = 'audio/mpeg'
            
            # Get file size
            file_size = os.path.getsize(file_path)
            
            # Send response
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(file_size))
            self.send_header('Accept-Ranges', 'bytes')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            # Stream file
            with open(file_path, 'rb') as f:
                self.wfile.write(f.read())
        except Exception as e:
            print(f"Error streaming audio: {e}")
            self.send_error(500)
    
    def handle_inquiry_submission(self):
        """Handle exclusive license inquiry submission."""
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            data = json.loads(body)
            
            # Validate required fields
            required_fields = ['beatId', 'name', 'email', 'offer']
            if not all(field in data for field in required_fields):
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'Missing required fields'}).encode())
                return
            
            # Validate email format
            if '@' not in data['email'] or '.' not in data['email']:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'Invalid email format'}).encode())
                return
            
            # Save inquiry
            inquiry_id = self.db.save_inquiry(
                data['beatId'],
                data['name'],
                data['email'],
                data['offer']
            )
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({
                'success': True,
                'inquiryId': inquiry_id,
                'message': 'Inquiry submitted successfully'
            }).encode())
        except json.JSONDecodeError:
            self.send_error(400)
        except Exception as e:
            print(f"Error submitting inquiry: {e}")
            self.send_error(500)
    
    def serve_file(self, file_name, content_type):
        """Serve static file."""
        try:
            file_path = os.path.join(os.path.dirname(__file__), file_name)
            
            if not os.path.exists(file_path):
                self.send_error(404)
                return
            
            with open(file_path, 'rb') as f:
                content = f.read()
            
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        except Exception as e:
            print(f"Error serving file: {e}")
            self.send_error(500)
    
    def do_OPTIONS(self):
        """Handle CORS preflight requests."""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def log_message(self, format, *args):
        """Custom logging."""
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {format % args}")


def main():
    """Start the beat store server."""
    server_address = ('', PORT)
    httpd = HTTPServer(server_address, BeatStoreHandler)
    
    print(f"""
╔════════════════════════════════════════════╗
║     Astro Blacksmith Beats Server          ║
║     Modern Beat Store Platform             ║
╚════════════════════════════════════════════╝

✓ Server running on http://localhost:{PORT}
✓ Beats folder: {os.path.abspath(BEATS_FOLDER)}
✓ Database: {os.path.abspath(DB_PATH)}

📁 Scanning for beats...
🎵 Supported formats: MP3, WAV, M4A, FLAC, OGG

API Endpoints:
  GET  /api/beats              - List all beats
  GET  /api/beats/<id>         - Get beat details
  GET  /api/audio/<filename>   - Stream audio file
  POST /api/inquiry            - Submit exclusive inquiry

Press Ctrl+C to stop the server
    """)
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n✓ Server stopped")
        httpd.server_close()


if __name__ == '__main__':
    main()