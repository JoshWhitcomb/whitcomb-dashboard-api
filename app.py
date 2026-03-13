import os
import json
import requests
import pg8000
import pg8000.native
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
import os

API_SECRET_KEY = os.environ.get('API_SECRET_KEY')

def check_api_key():
    if request.method == 'OPTIONS':
        return None
    key = request.headers.get('X-API-Key')
    if not key or key != API_SECRET_KEY:
        return jsonify({'error': 'Unauthorized'}), 401
from urllib.parse import urlparse

load_dotenv()

app = Flask(__name__)
CORS(app, origins=["https://dashboard.joshwhitcomb.com", "http://localhost:3000"])
app.before_request(check_api_key)


def get_db():
    url = urlparse(os.environ["DATABASE_URL"])
    return pg8000.connect(
        host=url.hostname,
        port=url.port or 5432,
        database=url.path[1:],
        user=url.username,
        password=url.password
    )

def fetchall_dict(cursor):
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            balance NUMERIC(12,2),
            owner TEXT,
            type TEXT,
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS net_worth_history (
            id SERIAL PRIMARY KEY,
            date TEXT NOT NULL UNIQUE,
            value NUMERIC(12,2),
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS income (
            id SERIAL PRIMARY KEY,
            key TEXT NOT NULL UNIQUE,
            value NUMERIC(12,2)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS contributions (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            amount NUMERIC(10,2)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wheel_trades (
            id SERIAL PRIMARY KEY,
            ticker TEXT NOT NULL,
            date TEXT,
            type TEXT,
            strike NUMERIC(8,2),
            expiry TEXT,
            premium NUMERIC(10,2),
            contracts INTEGER,
            status TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wheel_shares (
            id SERIAL PRIMARY KEY,
            ticker TEXT NOT NULL UNIQUE,
            shares INTEGER,
            cost_basis NUMERIC(8,2)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weight_log (
            id SERIAL PRIMARY KEY,
            date TEXT NOT NULL UNIQUE,
            weight NUMERIC(5,1),
            steps INTEGER,
            sleep_minutes INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS labs (
            id SERIAL PRIMARY KEY,
            date TEXT NOT NULL UNIQUE,
            a1c NUMERIC(4,2),
            fasting_glucose INTEGER,
            triglycerides INTEGER,
            hdl INTEGER,
            ldl INTEGER,
            total_cholesterol INTEGER,
            systolic INTEGER,
            diastolic INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meditation_log (
            id SERIAL PRIMARY KEY,
            date TEXT NOT NULL UNIQUE,
            minutes NUMERIC(6,1)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS goals (
            id SERIAL PRIMARY KEY,
            year INTEGER NOT NULL,
            name TEXT NOT NULL,
            target NUMERIC(8,1),
            current NUMERIC(8,1),
            unit TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS habit_log (
            id SERIAL PRIMARY KEY,
            date TEXT NOT NULL,
            habit TEXT NOT NULL,
            value NUMERIC(8,2) NOT NULL DEFAULT 1,
            UNIQUE(date, habit)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            author TEXT,
            started_date TEXT,
            finished_date TEXT,
            status TEXT DEFAULT 'reading'
        )
    """)
    for col, coltype in [('steps', 'INTEGER'), ('sleep_minutes', 'INTEGER')]:
        try:
            cur.execute(f"ALTER TABLE weight_log ADD COLUMN {col} {coltype}")
            conn.commit()
        except Exception:
            conn.rollback()
    try:
        cur.execute("ALTER TABLE books ADD COLUMN rating INTEGER")
        conn.commit()
    except Exception:
        conn.rollback()
    for col, coltype in [('systolic', 'INTEGER'), ('diastolic', 'INTEGER'), ('total_cholesterol', 'INTEGER')]:
        try:
            cur.execute(f"ALTER TABLE labs ADD COLUMN {col} {coltype}")
            conn.commit()
        except Exception:
            conn.rollback()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS connect_people (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            relationship TEXT,
            cadence_days INTEGER DEFAULT 30,
            last_contact TEXT,
            notes TEXT DEFAULT '',
            gift_ideas TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS connect_logs (
            id SERIAL PRIMARY KEY,
            date TEXT NOT NULL,
            type TEXT NOT NULL,
            notes TEXT DEFAULT '',
            people TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

@app.route("/api/accounts", methods=["GET"])
def get_accounts():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, name, balance, owner, type FROM accounts ORDER BY id")
    rows = fetchall_dict(cur)
    cur.close(); conn.close()
    return jsonify(rows)

@app.route("/api/accounts", methods=["POST"])
def save_accounts():
    accounts = request.json
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM accounts")
    for a in accounts:
        cur.execute(
            "INSERT INTO accounts (id, name, balance, owner, type) VALUES (%s, %s, %s, %s, %s)",
            (a["id"], a["name"], a["balance"], a["owner"], a["type"])
        )
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/net-worth-history", methods=["GET"])
def get_history():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT date, value FROM net_worth_history ORDER BY date")
    rows = fetchall_dict(cur)
    cur.close(); conn.close()
    return jsonify(rows)

@app.route("/api/net-worth-history", methods=["POST"])
def save_history():
    rows = request.json
    conn = get_db()
    cur = conn.cursor()
    for r in rows:
        cur.execute("""
            INSERT INTO net_worth_history (date, value)
            VALUES (%s, %s)
            ON CONFLICT (date) DO UPDATE SET value = EXCLUDED.value
        """, (r["date"], r["value"]))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/income", methods=["GET"])
def get_income():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT key, value FROM income")
    rows = {r[0]: float(r[1]) for r in cur.fetchall()}
    cur.close(); conn.close()
    return jsonify(rows)

@app.route("/api/income", methods=["POST"])
def save_income():
    data = request.json
    conn = get_db()
    cur = conn.cursor()
    for key, value in data.items():
        cur.execute("""
            INSERT INTO income (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, (key, value))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/contributions", methods=["GET"])
def get_contributions():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, name, amount FROM contributions ORDER BY id")
    rows = fetchall_dict(cur)
    cur.close(); conn.close()
    return jsonify(rows)

@app.route("/api/contributions", methods=["POST"])
def save_contributions():
    data = request.json
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM contributions")
    for c in data:
        cur.execute("INSERT INTO contributions (id, name, amount) VALUES (%s, %s, %s)",
                    (c["id"], c["name"], c["amount"]))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/wheel/trades", methods=["GET"])
def get_trades():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, ticker, date, type, strike, expiry, premium, contracts, status FROM wheel_trades ORDER BY date")
    rows = fetchall_dict(cur)
    cur.close(); conn.close()
    return jsonify(rows)

@app.route("/api/wheel/trades", methods=["POST"])
def save_trade():
    t = request.json
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO wheel_trades (ticker, date, type, strike, expiry, premium, contracts, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (t["ticker"], t["date"], t["type"], t["strike"], t["expiry"], t["premium"], t["contracts"], t["status"]))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/wheel/shares", methods=["GET"])
def get_shares():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, ticker, shares, cost_basis FROM wheel_shares")
    rows = fetchall_dict(cur)
    cur.close(); conn.close()
    return jsonify(rows)

@app.route("/api/wheel/shares", methods=["POST"])
def save_shares():
    data = request.json
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO wheel_shares (ticker, shares, cost_basis)
        VALUES (%s, %s, %s)
        ON CONFLICT (ticker) DO UPDATE SET shares = EXCLUDED.shares, cost_basis = EXCLUDED.cost_basis
    """, (data["ticker"], data["shares"], data["costBasis"]))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/health/weight", methods=["GET"])
def get_weight():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT date, weight, steps, sleep_minutes FROM weight_log ORDER BY date")
    rows = fetchall_dict(cur)
    cur.close(); conn.close()
    return jsonify(rows)

@app.route("/api/health/weight", methods=["POST"])
def save_weight():
    data = request.json
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO weight_log (date, weight)
        VALUES (%s, %s)
        ON CONFLICT (date) DO UPDATE SET weight = EXCLUDED.weight
    """, (data["date"], data["weight"]))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/health/labs", methods=["GET"])
def get_labs():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, date, a1c, fasting_glucose, triglycerides, hdl, ldl, total_cholesterol, systolic, diastolic FROM labs ORDER BY date")
    rows = fetchall_dict(cur)
    cur.close(); conn.close()
    return jsonify(rows)

@app.route("/api/health/labs", methods=["POST"])
def save_labs():
    data = request.json
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO labs (date, a1c, fasting_glucose, triglycerides, hdl, ldl, total_cholesterol, systolic, diastolic)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE SET
            a1c = EXCLUDED.a1c,
            fasting_glucose = EXCLUDED.fasting_glucose,
            triglycerides = EXCLUDED.triglycerides,
            hdl = EXCLUDED.hdl,
            ldl = EXCLUDED.ldl,
            total_cholesterol = EXCLUDED.total_cholesterol,
            systolic = EXCLUDED.systolic,
            diastolic = EXCLUDED.diastolic
    """, (data["date"], data.get("a1c"), data.get("fastingGlucose"),
          data.get("triglycerides"), data.get("hdl"), data.get("ldl"),
          data.get("totalCholesterol"), data.get("systolic"), data.get("diastolic")))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/meditation", methods=["GET"])
def get_meditation():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT date, minutes FROM meditation_log ORDER BY date")
    rows = fetchall_dict(cur)
    cur.close(); conn.close()
    return jsonify(rows)

@app.route("/api/meditation", methods=["POST"])
def save_meditation():
    data = request.json
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO meditation_log (date, minutes)
        VALUES (%s, %s)
        ON CONFLICT (date) DO UPDATE SET minutes = EXCLUDED.minutes
    """, (data["date"], data["minutes"]))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/sync", methods=["POST"])
def sync():
    data = request.json
    date = data.get("date", "").strip()
    if not date:
        return jsonify({"error": "date required"}), 400
    try:
        from dateutil import parser as dateparser
        date = dateparser.parse(date).strftime("%Y-%m-%d")
    except:
        pass

    def to_float(v):
        try:
            if v in (None, '', 'null'): return None
            return float(str(v).split()[0])
        except: return None
    def to_int(v):
        try: return int(float(v)) if v not in (None, '', 'null') else None
        except: return None

    conn = get_db()
    cur = conn.cursor()

    weight = to_float(data.get("weight"))
    steps  = to_int(data.get("steps"))
    sleep  = to_int(data.get("sleepMinutes"))

    cur.execute("INSERT INTO weight_log (date) VALUES (%s) ON CONFLICT (date) DO NOTHING", (date,))
    if weight is not None:
        cur.execute("UPDATE weight_log SET weight = %s WHERE date = %s", (weight, date))
    if steps is not None:
        cur.execute("UPDATE weight_log SET steps = %s WHERE date = %s", (steps, date))
    if sleep is not None:
        cur.execute("UPDATE weight_log SET sleep_minutes = %s WHERE date = %s", (sleep, date))

    mindful = to_float(data.get("mindfulMinutes"))
    if mindful is not None and mindful > 0:
        cur.execute("""
            INSERT INTO meditation_log (date, minutes)
            VALUES (%s, %s)
            ON CONFLICT (date) DO UPDATE SET minutes = EXCLUDED.minutes
        """, (date, mindful))

    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True, "date": date})

@app.route("/api/health/weight/<path:date>", methods=["DELETE"])
def delete_weight(date):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM weight_log WHERE date = %s", (date,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/health/weight/delete-malformed", methods=["DELETE"])
def delete_malformed_weight():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM weight_log WHERE date NOT SIMILAR TO '[0-9]{4}-[0-9]{2}-[0-9]{2}'")
    deleted = cur.rowcount
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True, "deleted": deleted})

@app.route("/api/meditation/<date>", methods=["DELETE"])
def delete_meditation(date):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM meditation_log WHERE date = %s", (date,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/goals", methods=["GET"])
def get_goals():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, year, name, target, current, unit FROM goals ORDER BY id")
    rows = fetchall_dict(cur)
    cur.close(); conn.close()
    return jsonify(rows)

@app.route("/api/goals", methods=["POST"])
def save_goals():
    data = request.json
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM goals WHERE year = %s", (data["year"],))
    for g in data["items"]:
        cur.execute("""
            INSERT INTO goals (year, name, target, current, unit)
            VALUES (%s, %s, %s, %s, %s)
        """, (data["year"], g["name"], g["target"], g["current"], g["unit"]))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/habits", methods=["GET"])
def get_habits():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT date, habit, value FROM habit_log ORDER BY date")
    rows = fetchall_dict(cur)
    cur.close(); conn.close()
    return jsonify(rows)

@app.route("/api/habits", methods=["POST"])
def save_habit():
    data = request.json
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO habit_log (date, habit, value)
        VALUES (%s, %s, %s)
        ON CONFLICT (date, habit) DO UPDATE SET value = habit_log.value + EXCLUDED.value
    """, (data["date"], data["habit"], data["value"]))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/habits/bulk-delete", methods=["POST"])
def bulk_delete_habits():
    data = request.json
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM habit_log WHERE habit = %s AND date < %s",
        (data["habit"], data["before_date"]))
    deleted = cur.rowcount
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True, "deleted": deleted})

@app.route("/api/habits/<date>/<habit>", methods=["DELETE"])
def delete_habit(date, habit):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM habit_log WHERE date = %s AND habit = %s", (date, habit))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/settings", methods=["GET"])
def get_settings():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT key, value FROM settings")
    rows = {r[0]: r[1] for r in cur.fetchall()}
    cur.close(); conn.close()
    return jsonify(rows)

@app.route("/api/settings", methods=["POST"])
def save_settings():
    data = request.json
    conn = get_db()
    cur = conn.cursor()
    for key, value in data.items():
        cur.execute("""
            INSERT INTO settings (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, (key, str(value)))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/books", methods=["GET"])
def get_books():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, title, author, started_date, finished_date, status, rating FROM books ORDER BY id DESC")
    rows = fetchall_dict(cur)
    cur.close(); conn.close()
    return jsonify(rows)

@app.route("/api/books", methods=["POST"])
def add_book():
    data = request.json
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO books (title, author, started_date, status)
        VALUES (%s, %s, %s, %s)
        RETURNING id
    """, (data["title"], data.get("author", ""), data.get("started_date", ""), data.get("status", "reading")))
    new_id = cur.fetchone()[0]
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True, "id": new_id})

@app.route("/api/books/<int:book_id>", methods=["PATCH"])
def update_book(book_id):
    data = request.json
    conn = get_db()
    cur = conn.cursor()
    fields = []
    values = []
    for field in ["title", "author", "started_date", "finished_date", "status", "rating"]:
        if field in data:
            fields.append(f"{field} = %s")
            values.append(data[field])
    if fields:
        values.append(book_id)
        cur.execute(f"UPDATE books SET {', '.join(fields)} WHERE id = %s", values)
        conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/books/<int:book_id>", methods=["DELETE"])
def delete_book(book_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM books WHERE id = %s", (book_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/identify-book", methods=["POST"])
def identify_book():
    data = request.json
    image_data = data.get("image")
    media_type = data.get("media_type", "image/jpeg")
    if not image_data:
        return jsonify({"error": "No image provided"}), 400
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
    if not anthropic_key:
        return jsonify({"error": "Anthropic API key not configured"}), 500
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "Content-Type": "application/json",
            "x-api-key": anthropic_key,
            "anthropic-version": "2023-06-01",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 200,
            "messages": [{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {"type": "base64", "media_type": media_type, "data": image_data}
                    },
                    {
                        "type": "text",
                        "text": "This is a photo of a book cover. Identify the book title and author. Respond with ONLY a JSON object like: {\"title\": \"...\", \"author\": \"...\"}. If you cannot identify it, respond with {\"title\": \"\", \"author\": \"\"}."
                    }
                ]
            }]
        }
    )
    result = resp.json()
    text = result.get("content", [{}])[0].get("text", "{}")
    clean = text.replace("```json", "").replace("```", "").strip()
    try:
        parsed = json.loads(clean)
    except Exception:
        parsed = {"title": "", "author": ""}
    return jsonify(parsed)

@app.route("/api/backfill-spanish", methods=["POST"])
def backfill_spanish():
    from datetime import date, timedelta
    auth = check_api_key()
    if auth: return auth
    conn = get_db()
    cur = conn.cursor()
    start = date(2019, 6, 9)
    end = date(2026, 3, 7)
    d = start
    count = 0
    while d <= end:
        cur.execute(
            "INSERT INTO habit_log (date, habit, value) VALUES (%s, %s, %s) ON CONFLICT (date, habit) DO NOTHING",
            (d.isoformat(), 'spanish', 1)
        )
        d += timedelta(days=1)
        count += 1
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"ok": True, "inserted": count})

@app.route("/api/health-check")
def health_check():
    return jsonify({"status": "ok"})

init_db()


@app.route('/api/finance/retirement-debug', methods=['GET'])
def get_retirement_debug():
    try:
        service = get_sheets_service()
        RETIREMENT_ID = '1ep3Ax2Vg3awiDGi0_l805LnW0z52HnTjcaColNHMQdw'
        meta = service.spreadsheets().get(spreadsheetId=RETIREMENT_ID).execute()
        sheets = [s['properties']['title'] for s in meta['sheets']]
        result = service.spreadsheets().values().get(
            spreadsheetId=RETIREMENT_ID, range='2026!A1:H45'
        ).execute()
        return jsonify({'tabs': sheets, 'sample': result.get('values', [])})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/finance/retirement', methods=['GET'])
def get_retirement():
    try:
        service = get_sheets_service()
        RETIREMENT_ID = '1ep3Ax2Vg3awiDGi0_l805LnW0z52HnTjcaColNHMQdw'

        # Tab-specific row mappings (0-indexed)
        # date_row: row containing the snapshot date (col B/E/H)
        # total_row: row containing Investment/Savings Total
        tab_config = {
            '2021': {'date_row': 2, 'total_row': 17},
            '2022': {'date_row': 2, 'total_row': 18},
            '2023': {'date_row': 1, 'total_row': 16},
            '2024': {'date_row': 1, 'total_row': 17},
            '2025': {'date_row': 1, 'total_row': 18},
            '2026': {'date_row': 1, 'total_row': 18},
        }

        snapshots = []

        def parse_dollar(val):
            if not val:
                return 0.0
            try:
                return float(str(val).replace('$', '').replace(',', '').strip())
            except:
                return 0.0

        for tab, cfg in tab_config.items():
            try:
                result = service.spreadsheets().values().get(
                    spreadsheetId=RETIREMENT_ID, range=f'{tab}!A1:H45'
                ).execute()
                rows = result.get('values', [])

                def get_cell(row_idx, col_idx):
                    if row_idx >= len(rows):
                        return ''
                    row = rows[row_idx]
                    if col_idx >= len(row):
                        return ''
                    return row[col_idx]

                date_row  = cfg['date_row']
                total_row = cfg['total_row']

                for col_offset in [0, 3, 6]:
                    date_val  = get_cell(date_row,  col_offset + 1)
                    total_val = get_cell(total_row, col_offset + 1)
                    if not date_val or not total_val:
                        continue
                    total = parse_dollar(total_val)
                    if total <= 0:
                        continue

                    snapshots.append({
                        'date':  date_val,
                        'tab':   tab,
                        'total': total,
                    })
            except Exception:
                continue

        from datetime import datetime
        def parse_date(d):
            for fmt in ['%m/%d/%Y', '%m/%d/%y', '%m/%d/%y']:
                try:
                    return datetime.strptime(d.strip(), fmt)
                except:
                    pass
            return datetime.min

        snapshots.sort(key=lambda s: parse_date(s['date']))

        # Deduplicate
        seen = set()
        unique = []
        for s in snapshots:
            if s['date'] not in seen:
                seen.add(s['date'])
                unique.append(s)

        # Get latest snapshot for income/projection data from 2026 tab
        latest_result = service.spreadsheets().values().get(
            spreadsheetId=RETIREMENT_ID, range='2026!A1:H45'
        ).execute()
        latest_rows = latest_result.get('values', [])

        def get_latest(row_idx, col_offset=6):
            if row_idx >= len(latest_rows):
                return 0.0
            row = latest_rows[row_idx]
            if col_offset + 1 >= len(row):
                return 0.0
            return parse_dollar(row[col_offset + 1])

        latest = {
            'fv6':               get_latest(19),
            'fv10':              get_latest(20),
            'income_current':    get_latest(29),
            'income_6':          get_latest(30),
            'income_10':         get_latest(31),
            'pension_jenny':     get_latest(25),
            'pension_josh':      get_latest(26),
            'ss_josh':           get_latest(27),
            'ss_jenny':          get_latest(28),
            'contributions_monthly': get_latest(41),
        }

        return jsonify({'snapshots': unique, 'latest': latest})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)


# ── CONNECT ──────────────────────────────────────────────────────────────────

@app.route("/api/connect/people", methods=["GET"])
def get_connect_people():
    auth = check_api_key()
    if auth: return auth
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, name, relationship, cadence_days, last_contact, notes, gift_ideas FROM connect_people ORDER BY name")
    rows = fetchall_dict(cur)
    cur.close(); conn.close()
    return jsonify(rows)

@app.route("/api/connect/people", methods=["POST"])
def add_connect_person():
    auth = check_api_key()
    if auth: return auth
    d = request.json
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO connect_people (name, relationship, cadence_days, last_contact, notes, gift_ideas) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
        (d.get('name'), d.get('relationship'), d.get('cadence_days', 30), d.get('last_contact'), d.get('notes',''), d.get('gift_ideas',''))
    )
    new_id = cur.fetchone()[0]
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True, "id": new_id})

@app.route("/api/connect/people/<int:person_id>", methods=["PATCH"])
def update_connect_person(person_id):
    auth = check_api_key()
    if auth: return auth
    d = request.json
    allowed = ['name','relationship','cadence_days','last_contact','notes','gift_ideas']
    fields = {k: v for k, v in d.items() if k in allowed}
    if not fields:
        return jsonify({"error": "No valid fields"}), 400
    conn = get_db()
    cur = conn.cursor()
    sets = ", ".join(f"{k} = %s" for k in fields)
    cur.execute(f"UPDATE connect_people SET {sets} WHERE id = %s", list(fields.values()) + [person_id])
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/connect/people/<int:person_id>", methods=["DELETE"])
def delete_connect_person(person_id):
    auth = check_api_key()
    if auth: return auth
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM connect_people WHERE id = %s", (person_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/connect/logs", methods=["GET"])
def get_connect_logs():
    auth = check_api_key()
    if auth: return auth
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, date, type, notes, people FROM connect_logs ORDER BY date DESC")
    rows = fetchall_dict(cur)
    cur.close(); conn.close()
    return jsonify(rows)

@app.route("/api/connect/logs", methods=["POST"])
def add_connect_log():
    auth = check_api_key()
    if auth: return auth
    d = request.json
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO connect_logs (date, type, notes, people) VALUES (%s, %s, %s, %s) RETURNING id",
        (d.get('date'), d.get('type'), d.get('notes',''), d.get('people',''))
    )
    new_id = cur.fetchone()[0]
    # Update last_contact for each person involved
    if d.get('person_ids'):
        for pid in d['person_ids']:
            cur.execute("UPDATE connect_people SET last_contact = %s WHERE id = %s", (d.get('date'), pid))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True, "id": new_id})

@app.route("/api/connect/logs/<int:log_id>", methods=["DELETE"])
def delete_connect_log(log_id):
    auth = check_api_key()
    if auth: return auth
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM connect_logs WHERE id = %s", (log_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})

# Google Sheets debt tracking
from google.oauth2 import service_account
from googleapiclient.discovery import build

SPREADSHEET_ID = '1af5BFxJSOvi15DaceBwawVcFEc5J_0qnwCozgq8-yg4'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

def get_sheets_service():
    creds_json = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
    if creds_json:
        import json as _json
        creds_info = _json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    else:
        creds = service_account.Credentials.from_service_account_file(
            os.path.join(os.path.dirname(__file__), 'whitcomb-dashboard-b01ae220f667.json'), scopes=SCOPES)
    return build('sheets', 'v4', credentials=creds)

@app.route("/api/finance/debt", methods=["GET"])
def get_debt():
    auth = check_api_key()
    if auth: return auth
    try:
        service = get_sheets_service()
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range='Sheet1!A:E'
        ).execute()
        rows = result.get('values', [])

        months = []
        i = 0
        while i < len(rows):
            row = rows[i]
            # Look for month header: cell B has "Month YYYY" pattern
            b = row[1].strip() if len(row) > 1 else ''
            import re
            if re.match(r'^[A-Za-z]+ \d{4}$', b):
                month_name = b
                debts = []
                i += 1
                # Skip until we hit the "What" header row
                while i < len(rows) and (len(rows[i]) < 2 or rows[i][1].strip() != 'What'):
                    i += 1
                i += 1  # skip the "What" header row
                # Read debt rows until blank or totals row
                while i < len(rows):
                    r = rows[i]
                    name = r[1].strip() if len(r) > 1 else ''
                    if not name or name.startswith('$'):
                        break
                    balance_str = r[2].strip() if len(r) > 2 else '0'
                    minimum_str = r[3].strip() if len(r) > 3 else '0'
                    paid = len(r) > 4 and r[4].strip().upper() == 'X'
                    # Strip $ and commas
                    balance = float(balance_str.replace('$','').replace(',','') or 0)
                    minimum = float(minimum_str.replace('$','').replace(',','') or 0)
                    debts.append({
                        'name': name,
                        'balance': balance,
                        'minimum': minimum,
                        'paid': paid
                    })
                    i += 1
                if debts:
                    months.append({'month': month_name, 'debts': debts})
            else:
                i += 1

        return jsonify(months)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

