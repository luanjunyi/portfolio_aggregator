"""Read access to the portfolio SQLite database.

Kept deliberately small. Future features (e.g. position tags) will add write
helpers here; the connection helper already opens the db read-write so those
drop in without restructuring.
"""
import os
import sqlite3

DB_PATH = os.environ.get(
    "PORTFOLIO_DB",
    "/Users/jluan/code/portfolio/portfolio.db",
)


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def latest_date(conn):
    row = conn.execute(
        "SELECT date FROM portfolio_snapshots ORDER BY date DESC LIMIT 1"
    ).fetchone()
    return row["date"] if row else None


def get_summary(conn, date):
    return conn.execute(
        "SELECT * FROM portfolio_snapshots WHERE date = ?", (date,)
    ).fetchone()


def get_holdings(conn, date):
    return conn.execute(
        "SELECT * FROM holdings_snapshots WHERE date = ? ORDER BY current_value DESC",
        (date,),
    ).fetchall()


def get_history(conn):
    return conn.execute(
        "SELECT date, total_value, total_cost_basis "
        "FROM portfolio_snapshots ORDER BY date"
    ).fetchall()


# --- Tags & dimensions -----------------------------------------------------
# A dimension groups tags (e.g. "sector" -> semiconductor, SaaS). A position
# (keyed by symbol, so tags persist across daily snapshots) can carry many
# tags. Deleting a dimension cascades to its tags and their assignments.

def init_schema():
    conn = get_conn()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tag_dimensions (
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS tags (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                dimension_id INTEGER NOT NULL REFERENCES tag_dimensions(id) ON DELETE CASCADE,
                name         TEXT NOT NULL,
                UNIQUE(dimension_id, name)
            );
            CREATE TABLE IF NOT EXISTS position_tags (
                symbol TEXT NOT NULL,
                tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
                PRIMARY KEY (symbol, tag_id)
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def fetch_tag_model():
    """Everything the UI needs in one shot: dimensions (with nested tags) and
    the symbol -> [tag_id] assignment map."""
    conn = get_conn()
    try:
        dims = conn.execute("SELECT id, name FROM tag_dimensions ORDER BY name").fetchall()
        tags = conn.execute("SELECT id, dimension_id, name FROM tags ORDER BY name").fetchall()
        pts = conn.execute("SELECT symbol, tag_id FROM position_tags").fetchall()
    finally:
        conn.close()

    tags_by_dim = {}
    for t in tags:
        tags_by_dim.setdefault(t["dimension_id"], []).append({"id": t["id"], "name": t["name"]})
    dimensions = [
        {"id": d["id"], "name": d["name"], "tags": tags_by_dim.get(d["id"], [])}
        for d in dims
    ]
    position_tags = {}
    for p in pts:
        position_tags.setdefault(p["symbol"], []).append(p["tag_id"])
    return {"dimensions": dimensions, "position_tags": position_tags}


def create_dimension(name):
    conn = get_conn()
    try:
        cur = conn.execute("INSERT INTO tag_dimensions(name) VALUES (?)", (name,))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def rename_dimension(dim_id, name):
    conn = get_conn()
    try:
        conn.execute("UPDATE tag_dimensions SET name = ? WHERE id = ?", (name, dim_id))
        conn.commit()
    finally:
        conn.close()


def delete_dimension(dim_id):
    conn = get_conn()
    try:
        conn.execute("DELETE FROM tag_dimensions WHERE id = ?", (dim_id,))
        conn.commit()
    finally:
        conn.close()


def create_tag(dimension_id, name):
    conn = get_conn()
    try:
        cur = conn.execute(
            "INSERT INTO tags(dimension_id, name) VALUES (?, ?)", (dimension_id, name)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def rename_tag(tag_id, name):
    conn = get_conn()
    try:
        conn.execute("UPDATE tags SET name = ? WHERE id = ?", (name, tag_id))
        conn.commit()
    finally:
        conn.close()


def delete_tag(tag_id):
    conn = get_conn()
    try:
        conn.execute("DELETE FROM tags WHERE id = ?", (tag_id,))
        conn.commit()
    finally:
        conn.close()


def add_position_tag(symbol, tag_id):
    conn = get_conn()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO position_tags(symbol, tag_id) VALUES (?, ?)",
            (symbol, tag_id),
        )
        conn.commit()
    finally:
        conn.close()


def remove_position_tag(symbol, tag_id):
    conn = get_conn()
    try:
        conn.execute(
            "DELETE FROM position_tags WHERE symbol = ? AND tag_id = ?", (symbol, tag_id)
        )
        conn.commit()
    finally:
        conn.close()
