import os
import pg8000
import pg8000.native
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv()

app = Flask(__name__)
CORS(app, origins=["https://dashboard.joshwhitcomb.com", "http://localhost:3000"])

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
    # Safe column migrations
    for col, coltype in [('steps', 'INTEGER'), ('sleep_minutes', 'INTEGER')]:
        try:
            cur.execute(f"ALTER TABLE weight_log ADD COLUMN {col} {coltype}")
            conn.commit()
        except Exception:
            conn.rollback()
    for col, coltype in [('systolic', 'INTEGER'), ('diastolic', 'INTEGER'), ('total_cholesterol', 'INTEGER')]:
        try:
            cur.execute(f"ALTER TABLE labs ADD COLUMN {col} {coltype}")
            conn.commit()
        except Exception:
            conn.rollback()
    conn.commit()
    cur.close()
    conn.close()

# ── Accounts ──────────────────────────────────────────
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

# ── Net Worth History ──────────────────────────────────
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

# ── Income ────────────────────────────────────────────
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

# ── Contributions ─────────────────────────────────────
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

# ── Wheel ─────────────────────────────────────────────
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

# ── Health ────────────────────────────────────────────
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

# ── iOS Shortcut Sync ─────────────────────────────────
# POST { date, weight, steps, sleepMinutes }
@app.route("/api/sync", methods=["POST"])
def sync():
    data = request.json
    date = data.get("date")
    if not date:
        return jsonify({"error": "date required"}), 400

    conn = get_db()
    cur = conn.cursor()

    # Ensure row exists for this date
    cur.execute("INSERT INTO weight_log (date) VALUES (%s) ON CONFLICT (date) DO NOTHING", (date,))

    # Safely convert values, treating empty strings as None
    def to_float(v):
        try: return float(v) if v not in (None, '', 'null') else None
        except: return None
    def to_int(v):
        try: return int(float(v)) if v not in (None, '', 'null') else None
        except: return None

    weight = to_float(data.get("weight"))
    steps = to_int(data.get("steps"))
    sleep = to_int(data.get("sleepMinutes"))

    if weight is not None:
        cur.execute("UPDATE weight_log SET weight = %s WHERE date = %s", (weight, date))
    if steps is not None:
        cur.execute("UPDATE weight_log SET steps = %s WHERE date = %s", (steps, date))
    if sleep is not None:
        cur.execute("UPDATE weight_log SET sleep_minutes = %s WHERE date = %s", (sleep, date))

    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True, "date": date})

# ── Goals ─────────────────────────────────────────────
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

# ── Settings ──────────────────────────────────────────
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

@app.route("/api/health-check")
def health_check():
    return jsonify({"status": "ok"})

init_db()

if __name__ == "__main__":
    app.run(debug=True)
