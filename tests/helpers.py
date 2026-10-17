"""Gemeinsame Hilfen der Tests: direkter Zugriff auf die Test-Datenbank."""
import os

import mysql.connector

DB = dict(
    host=os.getenv("DB_HOST", "127.0.0.1"),
    port=int(os.getenv("DB_PORT", "33307")),
    user=os.getenv("DB_USER", "app"),
    password=os.getenv("DB_PASS", "apppw"),
    database=os.getenv("DB_NAME", "dienstbuch_demo"),
)


def sql(q):
    """Fuehrt eine oder mehrere SQL-Anweisungen (durch ; getrennt) aus. Bei SELECT: Zeilen als Text (Spalten per Tab, NULL als 'NULL')."""
    conn = mysql.connector.connect(**DB)
    cur = conn.cursor()
    try:
        out = ""
        for stmt in (x for x in q.split(";") if x.strip()):  # ; kommt in den Tests nie innerhalb von Texten vor
            cur.execute(stmt)
            if cur.with_rows:
                out = "\n".join("\t".join("NULL" if v is None else str(v) for v in row) for row in cur.fetchall())
        conn.commit()
        return out
    finally:
        cur.close()
        conn.close()
