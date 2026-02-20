#!/usr/bin/env python3
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.io as pio
import os
import sys
from datetime import datetime

# Define paths
DB_PATH = "/Users/jluan/code/portfolio/portfolio.db"
OUTPUT_DIR = "/Users/jluan/code/portfolio/frontend"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "index.html")

def fmt_money(val):
    if val is None: return "$0.00"
    return f"${val:,.2f}"

def fmt_pct(val):
    if val is None: return "N/A"
    return f"{val*100:.2f}%"

def get_color_class(val):
    if val is None: return ""
    return "text-success" if val >= 0 else "text-danger"

def generate_report():
    print(f"Connecting to database at {DB_PATH}...")
    try:
        conn = sqlite3.connect(DB_PATH)
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)
        
    # 1. Fetch Historical Data
    chart_html = ""
    try:
        print("Fetching historical data...")
        df_history = pd.read_sql_query("SELECT date, total_value, total_cost_basis FROM portfolio_snapshots ORDER BY date", conn)
        
        if df_history.empty:
            print("No historical data found.")
            chart_html = "<div class='alert alert-info'>No historical data available yet.</div>"
        else:
            # Create Plotly Chart
            fig = px.line(df_history, x='date', y=['total_value', 'total_cost_basis'], 
                          title='Portfolio Value History',
                          labels={'value': 'Value ($)', 'date': 'Date', 'variable': 'Metric'},
                          template='plotly_white')
            
            fig.update_layout(
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                margin=dict(l=20, r=20, t=60, b=20),
                hovermode="x unified"
            )
            
            chart_html = pio.to_html(fig, full_html=False, include_plotlyjs='cdn')
            
    except Exception as e:
        print(f"Error generating chart: {e}")
        chart_html = f"<div class='alert alert-danger'>Error generating chart: {e}</div>"

    # 2. Fetch Latest Snapshot
    try:
        print("Fetching latest snapshot...")
        cursor = conn.cursor()
        cursor.execute("SELECT date FROM portfolio_snapshots ORDER BY date DESC LIMIT 1")
        result = cursor.fetchone()
        
        if not result:
            print("No snapshots found in database.")
            conn.close()
            return
            
        latest_date = result[0]
        print(f"Latest date: {latest_date}")
        
        # Fetch Portfolio Summary
        df_summary = pd.read_sql_query("SELECT * FROM portfolio_snapshots WHERE date = ?", conn, params=(latest_date,))
        summary_row = df_summary.iloc[0]
        
        # Fetch Holdings
        df_holdings = pd.read_sql_query("SELECT * FROM holdings_snapshots WHERE date = ? ORDER BY current_value DESC", conn, params=(latest_date,))
        
    except Exception as e:
        print(f"Error fetching data: {e}")
        conn.close()
        sys.exit(1)
        
    conn.close()
    
    # 3. Generate HTML
    print("Generating HTML report...")
    
    summary_day_change_cls = get_color_class(summary_row['day_change_dollars'])
    summary_unrealized_cls = get_color_class(summary_row['total_unrealized_gain_loss'])
    
    holdings_rows = ""
    for _, row in df_holdings.iterrows():
        day_change_cls = get_color_class(row['day_change_dollars'])
        unrealized_cls = get_color_class(row['unrealized_gain_loss'])
        
        holdings_rows += f"""
        <tr>
            <td class="fw-bold">{row['symbol']}</td>
            <td><small class="text-muted">{row['description']}</small></td>
            <td class="text-end">{row['quantity']:.4f}</td>
            <td class="text-end">{fmt_money(row['price'])}</td>
            <td class="text-end fw-bold">{fmt_money(row['current_value'])}</td>
            <td class="text-end {day_change_cls}">
                {fmt_money(row['day_change_dollars'])}
                <br><small>{fmt_pct(row['day_change_percent'])}</small>
            </td>
            <td class="text-end {unrealized_cls}">
                {fmt_money(row['unrealized_gain_loss'])}
                <br><small>{fmt_pct(row['unrealized_gain_loss_percent'])}</small>
            </td>
            <td class="text-end">{fmt_pct(row['portfolio_percentage'])}</td>
        </tr>
        """

    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Portfolio Report - {latest_date}</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body {{ background-color: #f8f9fa; padding-bottom: 40px; }}
        .metric-card {{ transition: transform 0.2s; border: none; shadow: 0 0.5rem 1rem rgba(0, 0, 0, 0.15); }}
        .metric-label {{ font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.5px; color: #6c757d; }}
        .metric-value {{ font-size: 1.75rem; font-weight: 700; }}
        .table-holdings th {{ background-color: #f1f3f5; font-size: 0.85rem; text-transform: uppercase; color: #495057; }}
        .table-holdings td {{ vertical-align: middle; }}
    </style>
</head>
<body>
    <div class="container py-4">
        <header class="d-flex justify-content-between align-items-center mb-5 pb-3 border-bottom">
            <h1 class="h3 m-0 text-primary">Portfolio Dashboard</h1>
            <span class="badge bg-secondary fs-6">{latest_date}</span>
        </header>

        <!-- Summary Cards -->
        <div class="row g-4 mb-5">
            <div class="col-md-3">
                <div class="card h-100 metric-card shadow-sm">
                    <div class="card-body text-center">
                        <div class="metric-label mb-2">Total Value</div>
                        <div class="metric-value text-dark">{fmt_money(summary_row['total_value'])}</div>
                    </div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card h-100 metric-card shadow-sm">
                    <div class="card-body text-center">
                        <div class="metric-label mb-2">Day Change</div>
                        <div class="metric-value {summary_day_change_cls}">
                            {fmt_money(summary_row['day_change_dollars'])}
                        </div>
                        <div class="small {summary_day_change_cls}">{fmt_pct(summary_row['day_change_percent'])}</div>
                    </div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card h-100 metric-card shadow-sm">
                    <div class="card-body text-center">
                        <div class="metric-label mb-2">Unrealized G/L</div>
                        <div class="metric-value {summary_unrealized_cls}">
                            {fmt_money(summary_row['total_unrealized_gain_loss'])}
                        </div>
                        <div class="small {summary_unrealized_cls}">{fmt_pct(summary_row['total_unrealized_gain_loss_percent'])}</div>
                    </div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card h-100 metric-card shadow-sm">
                    <div class="card-body text-center">
                        <div class="metric-label mb-2">Cost Basis</div>
                        <div class="metric-value text-secondary">{fmt_money(summary_row['total_cost_basis'])}</div>
                    </div>
                </div>
            </div>
        </div>

        <!-- History Chart -->
        <div class="card shadow-sm mb-5 border-0">
            <div class="card-body p-0">
                {chart_html}
            </div>
        </div>

        <!-- Holdings Table -->
        <div class="card shadow-sm border-0">
            <div class="card-header bg-white py-3">
                <h5 class="card-title m-0">Current Holdings</h5>
            </div>
            <div class="table-responsive">
                <table class="table table-hover table-holdings mb-0">
                    <thead>
                        <tr>
                            <th>Symbol</th>
                            <th>Description</th>
                            <th class="text-end">Qty</th>
                            <th class="text-end">Price</th>
                            <th class="text-end">Value</th>
                            <th class="text-end">Day Change</th>
                            <th class="text-end">Unrealized G/L</th>
                            <th class="text-end">% Port</th>
                        </tr>
                    </thead>
                    <tbody>
                        {holdings_rows}
                    </tbody>
                </table>
            </div>
        </div>
        
        <footer class="text-center text-muted mt-5">
            <small>Generated at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</small>
        </footer>
    </div>
</body>
</html>
    """
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        f.write(html_content)
    
    print(f"Report generated successfully at {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_report()
