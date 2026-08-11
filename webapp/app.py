"""Dynamic portfolio dashboard.

Live replacement for the static frontend/index.html that generate_report.py
used to emit. Reads portfolio.db on each request and renders the same view
(summary cards, value-history chart, sortable holdings, broker breakdown).

Exposes `app` (a Flask app). Designed to be mounted under a path prefix
(e.g. /portfolio) via Werkzeug DispatcherMiddleware: all URLs are built from
request.script_root, so it works both standalone and mounted.
"""
import sqlite3

import pandas as pd
import plotly.express as px
import plotly.io as pio
from flask import Flask, render_template, request, jsonify

from . import db

app = Flask(__name__)
db.init_schema()


def fmt_money(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "N/A"
    return f"${val:,.2f}"


def fmt_pct(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "N/A"
    return f"{val * 100:.2f}%"


def color_class(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return "text-success" if val >= 0 else "text-danger"


def build_chart(history_rows):
    if not history_rows:
        return "<div class='alert alert-info'>No historical data available yet.</div>"

    df = pd.DataFrame([dict(r) for r in history_rows])
    df["date_obj"] = pd.to_datetime(df["date"])
    earliest_val = df.iloc[0]["total_value"]
    latest_val = df.iloc[-1]["total_value"]
    days = (df["date_obj"].max() - df["date_obj"].min()).days
    years = days / 365.25

    total_return = (latest_val / earliest_val - 1) if earliest_val else 0
    if years > 0 and earliest_val > 0 and latest_val > 0:
        cagr = (latest_val / earliest_val) ** (1 / years) - 1
    else:
        cagr = 0

    title = (
        f"Portfolio Value History "
        f"(Total Return: {total_return * 100:.2f}%, CAGR: {cagr * 100:.2f}%)"
    )
    fig = px.line(
        df,
        x="date",
        y=["total_value", "total_cost_basis"],
        title=title,
        labels={"value": "Value ($)", "date": "Date", "variable": "Metric"},
        template="plotly_white",
    )
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=20, r=20, t=60, b=20),
        hovermode="x unified",
    )
    return pio.to_html(fig, full_html=False, include_plotlyjs="cdn")


def build_holdings(holding_rows):
    holdings = []
    accum_pct = 0.0
    for r in holding_rows:
        port_pct = r["portfolio_percentage"]
        if port_pct is not None and not pd.isna(port_pct):
            accum_pct += port_pct
        brokers = r["brokers"] if r["brokers"] else "{}"
        holdings.append(
            {
                "symbol": r["symbol"],
                "description": r["description"],
                "brokers": brokers,
                "quantity_disp": f"{r['quantity']:.4f}" if r["quantity"] is not None else "N/A",
                "unit_cost_disp": fmt_money(r["unit_cost"]),
                "price_disp": fmt_money(r["price"]),
                "cost_basis_disp": fmt_money(r["cost_basis"]),
                "value_disp": fmt_money(r["current_value"]),
                "day_change_disp": fmt_money(r["day_change_dollars"]),
                "day_change_pct_disp": fmt_pct(r["day_change_percent"]),
                "day_change_cls": color_class(r["day_change_dollars"]),
                "unrealized_disp": fmt_money(r["unrealized_gain_loss"]),
                "unrealized_pct_disp": fmt_pct(r["unrealized_gain_loss_percent"]),
                "unrealized_cls": color_class(r["unrealized_gain_loss"]),
                "port_pct_disp": fmt_pct(port_pct),
                "accum_pct_disp": fmt_pct(accum_pct),
                # raw values for client-side sorting
                "s_quantity": r["quantity"] or 0,
                "s_unit_cost": r["unit_cost"] or 0,
                "s_price": r["price"] or 0,
                "s_cost_basis": r["cost_basis"] or 0,
                "s_value": r["current_value"] or 0,
                "s_day_change": r["day_change_dollars"] or 0,
                "s_unrealized": r["unrealized_gain_loss"] or 0,
                "s_port_pct": port_pct or 0,
                "s_accum_pct": accum_pct,
            }
        )
    return holdings


@app.route("/")
def index():
    conn = db.get_conn()
    try:
        date = db.latest_date(conn)
        if not date:
            return render_template("dashboard.html", empty=True)
        summary = db.get_summary(conn, date)
        chart_html = build_chart(db.get_history(conn))
        holdings = build_holdings(db.get_holdings(conn, date))
    finally:
        conn.close()

    return render_template(
        "dashboard.html",
        empty=False,
        date=date,
        chart_html=chart_html,
        holdings=holdings,
        s={
            "total_value": fmt_money(summary["total_value"]),
            "day_change": fmt_money(summary["day_change_dollars"]),
            "day_change_pct": fmt_pct(summary["day_change_percent"]),
            "day_change_cls": color_class(summary["day_change_dollars"]),
            "unrealized": fmt_money(summary["total_unrealized_gain_loss"]),
            "unrealized_pct": fmt_pct(summary["total_unrealized_gain_loss_percent"]),
            "unrealized_cls": color_class(summary["total_unrealized_gain_loss"]),
            "cost_basis": fmt_money(summary["total_cost_basis"]),
        },
    )


# --- Tag/dimension API -----------------------------------------------------

def _name(req):
    return ((req.get_json(silent=True) or {}).get("name") or "").strip()


@app.route("/api/tag-model")
def api_tag_model():
    return jsonify(db.fetch_tag_model())


@app.route("/api/dimensions", methods=["POST"])
def api_create_dimension():
    name = _name(request)
    if not name:
        return jsonify(error="name required"), 400
    try:
        return jsonify(id=db.create_dimension(name)), 201
    except sqlite3.IntegrityError:
        return jsonify(error="A dimension with that name already exists."), 409


@app.route("/api/dimensions/<int:dim_id>", methods=["PATCH"])
def api_rename_dimension(dim_id):
    name = _name(request)
    if not name:
        return jsonify(error="name required"), 400
    try:
        db.rename_dimension(dim_id, name)
        return jsonify(ok=True)
    except sqlite3.IntegrityError:
        return jsonify(error="A dimension with that name already exists."), 409


@app.route("/api/dimensions/<int:dim_id>", methods=["DELETE"])
def api_delete_dimension(dim_id):
    db.delete_dimension(dim_id)
    return jsonify(ok=True)


@app.route("/api/tags", methods=["POST"])
def api_create_tag():
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    dimension_id = data.get("dimension_id")
    if not name or not dimension_id:
        return jsonify(error="name and dimension_id required"), 400
    try:
        return jsonify(id=db.create_tag(dimension_id, name)), 201
    except sqlite3.IntegrityError:
        return jsonify(error="That tag already exists in this dimension."), 409


@app.route("/api/tags/<int:tag_id>", methods=["PATCH"])
def api_rename_tag(tag_id):
    name = _name(request)
    if not name:
        return jsonify(error="name required"), 400
    try:
        db.rename_tag(tag_id, name)
        return jsonify(ok=True)
    except sqlite3.IntegrityError:
        return jsonify(error="That tag already exists in this dimension."), 409


@app.route("/api/tags/<int:tag_id>", methods=["DELETE"])
def api_delete_tag(tag_id):
    db.delete_tag(tag_id)
    return jsonify(ok=True)


@app.route("/api/position-tag", methods=["POST"])
def api_position_tag():
    data = request.get_json(silent=True) or {}
    symbol = (data.get("symbol") or "").strip()
    tag_id = data.get("tag_id")
    action = data.get("action")
    if not symbol or not tag_id or action not in ("add", "remove"):
        return jsonify(error="symbol, tag_id and action(add|remove) required"), 400
    if action == "add":
        db.add_position_tag(symbol, tag_id)
    else:
        db.remove_position_tag(symbol, tag_id)
    return jsonify(ok=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
