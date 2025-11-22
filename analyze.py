# analyze.py
import sqlite3
from datetime import datetime, timedelta

DB = 'logs.db'

def run():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 1) IPs with > 3 failed logins in a 5 minute window
    print("=== Brute-force candidates (failed logins >3 within 5 minutes) ===")
    q1 = """
    SELECT ip, username, COUNT(*) as fails,
           MIN(timestamp) as first_seen, MAX(timestamp) as last_seen
    FROM events
    WHERE event = 'login_failed'
    GROUP BY ip, username
    HAVING fails >= 3
    ORDER BY fails DESC
    """
    for row in cur.execute(q1):
        print(dict(row))

    # 2) Large downloads (bytes > 100MB)
    print("\n=== Large downloads (>100MB) ===")
    q2 = "SELECT timestamp, ip, username, bytes FROM events WHERE event='download' AND bytes > 100*1024*1024 ORDER BY bytes DESC"
    for row in cur.execute(q2):
        print(dict(row))

    # 3) Top IPs by events
    print("\n=== Top IPs by event count ===")
    q3 = "SELECT ip, COUNT(*) as cnt FROM events GROUP BY ip ORDER BY cnt DESC LIMIT 10"
    for row in cur.execute(q3):
        print(dict(row))

    conn.close()

if __name__ == '__main__':
    run()
