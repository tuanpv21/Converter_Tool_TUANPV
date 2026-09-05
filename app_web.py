#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Web UI Tool: Presto <-> Spark SQL Bi-directional Transpiler with Authentication
Tích hợp:
- Xác thực đăng nhập (Login / Logout / Change Password)
- Quản lý User bằng SQLite cục bộ + Hỗ trợ Cloud Secrets (ADMIN_USERNAME, ADMIN_PASSWORD)
- Bảo vệ API /api/convert bằng Session Token HttpOnly
- Giao diện Dark Theme hiện đại cho Ngân hàng
"""

import http.server
import socketserver
import json
import webbrowser
import os
import sys
import sqlite3
import hashlib
import secrets
import time
from http import cookies

try:
    from convert_presto_to_spark import convert_sql
except ImportError:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from convert_presto_to_spark import convert_sql

PORT = int(os.environ.get("PORT", 7860))
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "auth.db")

# Tài khoản mặc định từ biến môi trường (Ưu tiên khi deploy lên Cloud)
DEFAULT_ADMIN_USER = os.environ.get("ADMIN_USERNAME", "admin")
DEFAULT_ADMIN_PASS = os.environ.get("ADMIN_PASSWORD", "PublicBank@2026")
DEFAULT_ADMIN_PASS = os.environ.get("ADMIN_PASSWORD", "Tuanpv@2026")

# ==========================================
# 1. DATABASE & AUTHENTICATION HELPERS
# ==========================================
def hash_password(password: str, salt: str = None) -> tuple:
    if not salt:
        salt = secrets.token_hex(16)
    hashed = hashlib.sha256((password + salt).encode('utf-8')).hexdigest()
    return hashed, salt

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # Bảng users
    cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            salt TEXT NOT NULL,
            role TEXT DEFAULT 'user',
            created_at TEXT
        )
    ''')
    # Bảng sessions
    cur.execute('''
        CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            created_at REAL NOT NULL,
            expires_at REAL NOT NULL
        )
    ''')
    
    # Tạo user admin mặc định nếu chưa có
    cur.execute('SELECT username FROM users WHERE username = ?', (DEFAULT_ADMIN_USER,))
    if not cur.fetchone():
        h, s = hash_password(DEFAULT_ADMIN_PASS)
        cur.execute(
            'INSERT INTO users (username, password_hash, salt, role, created_at) VALUES (?, ?, ?, ?, ?)',
            (DEFAULT_ADMIN_USER, h, s, 'admin', time.strftime('%Y-%m-%d %H:%M:%S'))
        )
    conn.commit()
    conn.close()

def verify_credentials(username, password) -> bool:
    # 1. Kiểm tra khớp với biến môi trường Cloud Secrets trước
    if username == DEFAULT_ADMIN_USER and password == DEFAULT_ADMIN_PASS:
        return True
    
    # 2. Kiểm tra trong SQLite DB
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT password_hash, salt FROM users WHERE username = ?', (username,))
    row = cur.fetchone()
    conn.close()
    
    if not row:
        return False
    
    stored_hash, salt = row
    calculated_hash, _ = hash_password(password, salt)
    return secrets.compare_digest(stored_hash, calculated_hash)

def create_session(username) -> str:
    session_id = secrets.token_urlsafe(32)
    now = time.time()
    expires_at = now + (7 * 24 * 3600) # Hạn 7 ngày
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # Dọn dẹp session cũ hết hạn
    cur.execute('DELETE FROM sessions WHERE expires_at < ?', (now,))
    cur.execute('INSERT INTO sessions (session_id, username, created_at, expires_at) VALUES (?, ?, ?, ?)',
                (session_id, username, now, expires_at))
    conn.commit()
    conn.close()
    return session_id

def validate_session(session_id) -> str:
    if not session_id:
        return None
    now = time.time()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT username FROM sessions WHERE session_id = ? AND expires_at > ?', (session_id, now))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None

def destroy_session(session_id):
    if not session_id:
        return
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('DELETE FROM sessions WHERE session_id = ?', (session_id,))
    conn.commit()
    conn.close()

def update_user_password(username, new_password):
    h, s = hash_password(new_password)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('UPDATE users SET password_hash = ?, salt = ? WHERE username = ?', (h, s, username))
    conn.commit()
    conn.close()

# ==========================================
# 2. HTML INTERFACE WITH LOGIN & DASHBOARD
# ==========================================
HTML_PAGE = """<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Presto ⇄ Spark SQL Transpiler - Public Bank Vietnam</title>
  <title>Presto ⇄ Spark SQL Transpiler - TUANPV</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background: #0b1329; color: #e2e8f0; min-height: 100vh; display: flex; flex-direction: column; }
    
    /* Header */
    header { background: #131f37; border-bottom: 1px solid #1e293b; padding: 12px 24px; display: flex; justify-content: space-between; align-items: center; }
    .logo { font-size: 17px; font-weight: 700; color: #38bdf8; display: flex; align-items: center; gap: 8px; }
    .badge { background: #0284c7; color: #ffffff; font-size: 11px; padding: 3px 8px; border-radius: 9999px; font-weight: 600; }
    .user-pill { display: flex; align-items: center; gap: 10px; font-size: 13px; color: #94a3b8; }
    .user-pill strong { color: #f8fafc; }
    
    /* Buttons */
    button { font-family: inherit; font-size: 13px; font-weight: 600; padding: 8px 16px; border-radius: 6px; border: none; cursor: pointer; transition: all 0.15s ease; display: inline-flex; align-items: center; gap: 6px; }
    .btn-primary { background: #2563eb; color: white; }
    .btn-primary:hover { background: #1d4ed8; }
    .btn-switch { background: #0284c7; color: white; }
    .btn-switch:hover { background: #0369a1; }
    .btn-secondary { background: #1e293b; color: #cbd5e1; border: 1px solid #334155; }
    .btn-secondary:hover { background: #334155; }
    .btn-danger { background: #ef4444; color: white; }
    .btn-danger:hover { background: #dc2626; }
    .btn-copy { background: #059669; color: white; }
    .btn-copy:hover { background: #047857; }

    /* App Container */
    .main-container { flex: 1; display: flex; flex-direction: column; padding: 16px 24px; gap: 14px; }
    .toolbar { display: flex; gap: 10px; align-items: center; justify-content: space-between; flex-wrap: wrap; }
    .btn-group { display: flex; gap: 8px; align-items: center; }
    .panels { display: flex; gap: 16px; flex: 1; min-height: 520px; }
    .panel { flex: 1; display: flex; flex-direction: column; background: #131f37; border-radius: 8px; border: 1px solid #1e293b; overflow: hidden; }
    .panel-header { background: #0f172a; padding: 10px 16px; font-size: 13px; font-weight: 600; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #1e293b; }
    .panel-header.left { color: #38bdf8; }
    .panel-header.right { color: #34d399; }
    textarea { flex: 1; width: 100%; border: none; outline: none; background: #090e1a; color: #f8fafc; font-family: "Consolas", "Courier New", monospace; font-size: 13px; line-height: 1.5; padding: 16px; resize: none; tab-size: 4; }
    .status-bar { font-size: 12px; color: #94a3b8; padding: 4px 6px; }

    /* Modal & Login Form */
    .modal-overlay { position: fixed; inset: 0; background: rgba(3, 7, 18, 0.85); backdrop-filter: blur(4px); display: flex; align-items: center; justify-content: center; z-index: 999; }
    .auth-card { background: #131f37; border: 1px solid #334155; border-radius: 12px; padding: 32px; width: 100%; max-width: 400px; box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.5); }
    .auth-title { font-size: 20px; font-weight: 700; color: #f8fafc; margin-bottom: 6px; text-align: center; }
    .auth-sub { font-size: 13px; color: #94a3b8; margin-bottom: 24px; text-align: center; }
    .form-group { margin-bottom: 16px; }
    .form-label { display: block; font-size: 12px; font-weight: 600; color: #cbd5e1; margin-bottom: 6px; }
    .form-input { width: 100%; padding: 10px 14px; background: #090e1a; border: 1px solid #334155; border-radius: 6px; color: white; font-size: 14px; outline: none; transition: border-color 0.15s; }
    .form-input:focus { border-color: #38bdf8; }
    .auth-error { background: #7f1d1d; color: #fecaca; padding: 10px; border-radius: 6px; font-size: 12px; margin-bottom: 16px; display: none; }

    .toast { position: fixed; bottom: 20px; right: 20px; background: #10b981; color: white; padding: 10px 18px; border-radius: 6px; font-size: 13px; font-weight: 600; opacity: 0; transition: opacity 0.2s ease; pointer-events: none; z-index: 1000; }
    .toast.show { opacity: 1; }
  </style>
</head>
<body>

  <!-- LOGIN MODAL -->
  <div id="login-modal" class="modal-overlay">
    <div class="auth-card">
      <div style="text-align: center; margin-bottom: 12px; font-size: 32px;">🔐</div>
      <h2 class="auth-title">Xác Thực Hệ Thống</h2>
      <p class="auth-sub">Vui lòng đăng nhập để truy cập SQL Transpiler</p>
      
      <div id="login-error" class="auth-error"></div>

      <form id="login-form" onsubmit="handleLogin(event)">
        <div class="form-group">
          <label class="form-label">Tên đăng nhập (Username)</label>
          <input type="text" id="login-username" class="form-input" required placeholder="Nhập username..." />
        </div>
        <div class="form-group">
          <label class="form-label">Mật khẩu (Password)</label>
          <input type="password" id="login-password" class="form-input" required placeholder="Nhập mật khẩu..." />
        </div>
        <button type="submit" class="btn-primary" style="width: 100%; justify-content: center; padding: 11px; margin-top: 8px;">
          Đăng Nhập ➔
        </button>
      </form>
    </div>
  </div>

  <!-- CHANGE PASSWORD MODAL -->
  <div id="pwd-modal" class="modal-overlay" style="display: none;">
    <div class="auth-card">
      <h2 class="auth-title">Đổi Mật Khẩu</h2>
      <p class="auth-sub">Cập nhật mật khẩu tài khoản của bạn</p>
      <div id="pwd-error" class="auth-error"></div>
      <form onsubmit="handleChangePassword(event)">
        <div class="form-group">
          <label class="form-label">Mật khẩu hiện tại</label>
          <input type="password" id="old-pwd" class="form-input" required />
        </div>
        <div class="form-group">
          <label class="form-label">Mật khẩu mới</label>
          <input type="password" id="new-pwd" class="form-input" required minlength="6" />
        </div>
        <div style="display: flex; gap: 8px; margin-top: 16px;">
          <button type="submit" class="btn-primary" style="flex: 1; justify-content: center;">Lưu Mật Khẩu</button>
          <button type="button" class="btn-secondary" onclick="closePwdModal()">Hủy</button>
        </div>
      </form>
    </div>
  </div>

  <!-- APP HEADER -->
  <header>
    <div class="logo">
      <span>⚡ Presto ⇄ Spark SQL Transpiler 2 Chiều</span>
      <span class="badge">S3 Data Lake</span>
    </div>
    <div class="user-pill">
      <span>👤 Đăng nhập: <strong id="current-user">...</strong></span>
      <button class="btn-secondary" style="padding: 4px 10px; font-size: 11px;" onclick="openPwdModal()">🔑 Đổi MK</button>
      <button class="btn-danger" style="padding: 4px 10px; font-size: 11px;" onclick="handleLogout()">🚪 Đăng xuất</button>
    </div>
  </header>

  <!-- APP CONTENT -->
  <div class="main-container">
    <div class="toolbar">
      <div class="btn-group">
        <button id="btn-convert" class="btn-primary" onclick="convertSQL()">🚀 Chuyển đổi sang Spark SQL (Ctrl + Enter)</button>
        <button id="btn-toggle-mode" class="btn-switch" onclick="toggleMode()">⇄ Đổi chiều: Presto ➔ Spark SQL</button>
        <button class="btn-secondary" onclick="swapContent()">⇆ Đổi chỗ nội dung</button>
        <button class="btn-secondary" onclick="loadSample()">📄 Mẫu thử</button>
        <button class="btn-secondary" onclick="clearAll()">🗑️ Xóa trắng</button>
      </div>
      <div class="status-bar" id="status-bar">Sẵn sàng</div>
    </div>

    <div class="panels">
      <!-- PANEL INPUT -->
      <div class="panel">
        <div class="panel-header left">
          <span id="title-left">📥 INPUT: Presto SQL</span>
          <span id="sub-left" style="font-size: 11px; color: #64748b;">Nguồn: Presto / Trino / Athena</span>
        </div>
        <textarea id="sql-input" placeholder="Nhập hoặc dán câu lệnh SQL vào đây..."></textarea>
      </div>

      <!-- PANEL OUTPUT -->
      <div class="panel">
        <div class="panel-header right">
          <span id="title-right">📤 OUTPUT: Spark SQL</span>
          <button class="btn-copy" onclick="copyResult()">📋 Sao chép</button>
        </div>
        <textarea id="sql-output" readonly placeholder="Kết quả chuyển đổi sẽ hiển thị ở đây..."></textarea>
      </div>
    </div>
  </div>

  <div id="toast" class="toast">Thông báo</div>

  <script>
    let currentMode = 'presto2spark';

    const samplePresto = `-- Vi du truy van Presto tren S3
SELECT 
    cast(cust_id as varchar) as cust_id_str,
    json_extract_scalar(event_payload, '$.device_info.os') as os_type,
    date_diff('day', date_parse(sign_up_date, '%Y-%m-%d'), current_date) as active_days,
    cardinality(transaction_ids) as total_txns,
    contains(transaction_ids, 'TXN_VIP_999') as is_vip_txn,
    array_join(tag_names, ', ') as tags_str,
    approx_distinct(session_token) as approx_sessions
FROM customer_activity_logs
WHERE date_add('day', -30, current_date) <= date_parse(log_date, '%Y-%m-%d')
GROUP BY 1, 2, 3, 4, 5, 6, 7;`;

    const sampleSpark = `-- Vi du truy van Spark SQL tren S3
SELECT 
    cast(cust_id as string) as cust_id_str,
    get_json_object(event_payload, '$.device_info.os') as os_type,
    datediff(current_date(), to_date(sign_up_date, 'yyyy-MM-dd')) as active_days,
    size(transaction_ids) as total_txns,
    array_contains(transaction_ids, 'TXN_VIP_999') as is_vip_txn,
    concat_ws(', ', tag_names) as tags_str,
    approx_count_distinct(session_token) as approx_sessions
FROM customer_activity_logs
WHERE date_add(current_date(), -30) <= to_date(log_date, 'yyyy-MM-dd')
GROUP BY 1, 2, 3, 4, 5, 6, 7;`;

    // 1. CHECK SESSION KHI MO TRANG
    async function checkAuth() {
      try {
        const res = await fetch('/api/me');
        const data = await res.json();
        if (data.logged_in) {
          document.getElementById('login-modal').style.display = 'none';
          document.getElementById('current-user').innerText = data.username;
        } else {
          document.getElementById('login-modal').style.display = 'flex';
        }
      } catch (err) {
        document.getElementById('login-modal').style.display = 'flex';
      }
    }

    // 2. XU LY DANG NHAP
    async function handleLogin(e) {
      e.preventDefault();
      const u = document.getElementById('login-username').value.trim();
      const p = document.getElementById('login-password').value;
      const errBox = document.getElementById('login-error');
      errBox.style.display = 'none';

      try {
        const res = await fetch('/api/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ username: u, password: p })
        });
        const data = await res.json();
        if (data.status === 'ok') {
          document.getElementById('login-modal').style.display = 'none';
          document.getElementById('current-user').innerText = data.username;
          showToast("Đăng nhập thành công!");
          loadSample();
        } else {
          errBox.innerText = data.error || "Sai tên đăng nhập hoặc mật khẩu!";
          errBox.style.display = 'block';
        }
      } catch (err) {
        errBox.innerText = "Lỗi kết nối máy chủ!";
        errBox.style.display = 'block';
      }
    }

    // 3. XU LY DANG XUAT
    async function handleLogout() {
      await fetch('/api/logout', { method: 'POST' });
      document.getElementById('login-modal').style.display = 'flex';
      document.getElementById('login-username').value = '';
      document.getElementById('login-password').value = '';
      showToast("Đã đăng xuất");
    }

    // 4. DOI MAT KHAU
    function openPwdModal() {
      document.getElementById('pwd-modal').style.display = 'flex';
      document.getElementById('pwd-error').style.display = 'none';
    }
    function closePwdModal() {
      document.getElementById('pwd-modal').style.display = 'none';
    }
    async function handleChangePassword(e) {
      e.preventDefault();
      const oldPwd = document.getElementById('old-pwd').value;
      const newPwd = document.getElementById('new-pwd').value;
      const errBox = document.getElementById('pwd-error');
      errBox.style.display = 'none';

      try {
        const res = await fetch('/api/change-password', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ old_password: oldPwd, new_password: newPwd })
        });
        const data = await res.json();
        if (data.status === 'ok') {
          closePwdModal();
          showToast("Đổi mật khẩu thành công!");
        } else {
          errBox.innerText = data.error;
          errBox.style.display = 'block';
        }
      } catch (err) {
        errBox.innerText = "Lỗi kết nối máy chủ!";
        errBox.style.display = 'block';
      }
    }

    // 5. CHUYEN DOI SQL
    function updateUI() {
      if (currentMode === 'presto2spark') {
        document.getElementById('btn-toggle-mode').innerText = "⇄ Đổi chiều: Presto ➔ Spark SQL";
        document.getElementById('btn-toggle-mode').style.background = "#0284c7";
        document.getElementById('btn-convert').innerText = "🚀 Chuyển đổi sang Spark SQL (Ctrl + Enter)";
        document.getElementById('btn-convert').style.background = "#2563eb";
        document.getElementById('title-left').innerText = "📥 INPUT: Presto SQL";
        document.getElementById('sub-left').innerText = "Nguồn: Presto / Trino / Athena";
        document.getElementById('title-right').innerText = "📤 OUTPUT: Spark SQL";
      } else {
        document.getElementById('btn-toggle-mode').innerText = "⇄ Đổi chiều: Spark SQL ➔ Presto";
        document.getElementById('btn-toggle-mode').style.background = "#059669";
        document.getElementById('btn-convert').innerText = "🚀 Chuyển đổi sang Presto SQL (Ctrl + Enter)";
        document.getElementById('btn-convert').style.background = "#059669";
        document.getElementById('title-left').innerText = "📥 INPUT: Spark SQL";
        document.getElementById('sub-left').innerText = "Nguồn: Apache Spark SQL";
        document.getElementById('title-right').innerText = "📤 OUTPUT: Presto / Trino SQL";
      }
    }

    function toggleMode() {
      currentMode = currentMode === 'presto2spark' ? 'spark2presto' : 'presto2spark';
      updateUI();
      document.getElementById('status-bar').innerText = "Đã đổi chiều sang: " + currentMode.toUpperCase();
    }

    function swapContent() {
      const inVal = document.getElementById('sql-input').value;
      const outVal = document.getElementById('sql-output').value;
      document.getElementById('sql-input').value = outVal;
      document.getElementById('sql-output').value = inVal;
      toggleMode();
      document.getElementById('status-bar').innerText = "Đã đảo vị trí nội dung và chuyển hướng chuyển đổi!";
    }

    function loadSample() {
      document.getElementById('sql-input').value = currentMode === 'presto2spark' ? samplePresto : sampleSpark;
      document.getElementById('sql-output').value = '';
      document.getElementById('status-bar').innerText = "Đã nạp câu truy vấn mẫu.";
    }

    function clearAll() {
      document.getElementById('sql-input').value = '';
      document.getElementById('sql-output').value = '';
      document.getElementById('status-bar').innerText = "Đã xóa trắng.";
    }

    async function convertSQL() {
      const input = document.getElementById('sql-input').value.trim();
      if (!input) {
        alert("Vui lòng nhập câu lệnh SQL cần chuyển đổi!");
        return;
      }
      document.getElementById('status-bar').innerText = "Đang chuyển đổi...";

      try {
        const res = await fetch('/api/convert', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ query: input, mode: currentMode })
        });
        if (res.status === 401) {
          checkAuth();
          return;
        }
        const data = await res.json();
        if (data.status === 'ok') {
          document.getElementById('sql-output').value = data.result;
          document.getElementById('status-bar').innerText = "✅ Chuyển đổi thành công!";
        } else {
          document.getElementById('status-bar').innerText = "❌ Lỗi: " + data.error;
          alert("Lỗi chuyển đổi: " + data.error);
        }
      } catch (err) {
        document.getElementById('status-bar').innerText = "❌ Lỗi kết nối API";
      }
    }

    function copyResult() {
      const out = document.getElementById('sql-output').value;
      if (!out) return;
      navigator.clipboard.writeText(out);
      showToast("Đã sao chép vào Clipboard!");
    }

    function showToast(msg) {
      const toast = document.getElementById('toast');
      toast.innerText = msg;
      toast.classList.add('show');
      setTimeout(() => toast.classList.remove('show'), 2000);
    }

    document.addEventListener('keydown', (e) => {
      if (e.ctrlKey && e.key === 'Enter') {
        convertSQL();
      }
    });

    window.onload = () => {
      updateUI();
      checkAuth();
    };
  </script>
</body>
</html>
"""

# ==========================================
# 3. HTTP REQUEST HANDLER WITH AUTH
# ==========================================
class AuthRequestHandler(http.server.BaseHTTPRequestHandler):
    def get_session_token(self):
        cookie_header = self.headers.get("Cookie")
        if not cookie_header:
            return None
        c = cookies.SimpleCookie(cookie_header)
        if "session_token" in c:
            return c["session_token"].value
        return None

    def get_authenticated_user(self):
        token = self.get_session_token()
        return validate_session(token)

    def send_json(self, status_code, data, set_cookie_token=None, delete_cookie=False):
        resp_bytes = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(resp_bytes)))
        
        if set_cookie_token:
            # Cookie 7 ngày, HttpOnly để chống XSS
            c = cookies.SimpleCookie()
            c["session_token"] = set_cookie_token
            c["session_token"]["path"] = "/"
            c["session_token"]["httponly"] = True
            c["session_token"]["max-age"] = 7 * 24 * 3600
            for morsel in c.values():
                self.send_header("Set-Cookie", morsel.OutputString())
        elif delete_cookie:
            c = cookies.SimpleCookie()
            c["session_token"] = ""
            c["session_token"]["path"] = "/"
            c["session_token"]["max-age"] = 0
            for morsel in c.values():
                self.send_header("Set-Cookie", morsel.OutputString())

        self.end_headers()
        self.wfile.write(resp_bytes)

    def do_GET(self):
        if self.path == "/api/me":
            user = self.get_authenticated_user()
            if user:
                self.send_json(200, {"logged_in": True, "username": user})
            else:
                self.send_json(200, {"logged_in": False})
            return

        # Trang chính HTML
        html_bytes = HTML_PAGE.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(html_bytes)))
        self.end_headers()
        self.wfile.write(html_bytes)

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        
        try:
            req_data = json.loads(body.decode("utf-8")) if body else {}
        except Exception:
            req_data = {}

        # 1. API Đăng nhập
        if self.path == "/api/login":
            u = req_data.get("username", "").strip() or DEFAULT_ADMIN_USER
            p = req_data.get("password", "")
            if verify_credentials(u, p):
                token = create_session(u)
                self.send_json(200, {"status": "ok", "username": u}, set_cookie_token=token)
            else:
                self.send_json(401, {"status": "error", "error": "Sai mật khẩu hoặc tên đăng nhập"})
            return

        # 2. API Đăng xuất
        if self.path == "/api/logout":
            token = self.get_session_token()
            destroy_session(token)
            self.send_json(200, {"status": "ok"}, delete_cookie=True)
            return

        # 3. Yêu cầu đăng nhập cho các API bên dưới
        user = self.get_authenticated_user()
        if not user:
            self.send_json(401, {"status": "error", "error": "Yêu cầu đăng nhập để sử dụng tính năng này"})
            return

        # 4. API Chuyển đổi SQL
        if self.path == "/api/convert":
            query = req_data.get("query", "")
            mode = req_data.get("mode", "presto2spark")
            try:
                converted = convert_sql(query, mode=mode)
                self.send_json(200, {"status": "ok", "result": converted})
            except Exception as e:
                self.send_json(400, {"status": "error", "error": str(e)})
            return

        # 5. API Đổi mật khẩu
        if self.path == "/api/change-password":
            old_p = req_data.get("old_password", "")
            new_p = req_data.get("new_password", "")
            if not verify_credentials(user, old_p):
                self.send_json(400, {"status": "error", "error": "Mật khẩu hiện tại không đúng"})
                return
            if len(new_p) < 6:
                self.send_json(400, {"status": "error", "error": "Mật khẩu mới tối thiểu 6 ký tự"})
                return
            update_user_password(user, new_p)
            self.send_json(200, {"status": "ok"})
            return

        self.send_json(404, {"status": "error", "error": "Endpoint không tồn tại"})

    def log_message(self, format, *args):
        # Tắt logging nội dung query để bảo mật
        return

def main():
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except AttributeError:
            pass

    init_db()
    print("=" * 65)
    print("Presto <-> Spark SQL Transpiler with Authentication")
    print(f"Tai khoan mac dinh: {DEFAULT_ADMIN_USER} | Mat khau: {DEFAULT_ADMIN_PASS}")
    print(f"Dang mo trinh duyet tai: http://localhost:{PORT}")
    print("=" * 65)

    if not os.environ.get("PORT"):
        try:
            webbrowser.open(f"http://localhost:{PORT}")
        except Exception:
            pass

    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("0.0.0.0", PORT), AuthRequestHandler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nĐã tắt Web Server.")

if __name__ == "__main__":
    main()
