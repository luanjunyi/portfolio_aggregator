#!/usr/bin/env python3
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.io as pio
import os
import sys
import json
from datetime import datetime

# Define paths
DB_PATH = "/Users/jluan/code/portfolio/portfolio.db"
OUTPUT_DIR = "/Users/jluan/code/portfolio/frontend"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "index.html")

def fmt_money(val):
    if val is None or pd.isna(val): return "N/A"
    return f"${val:,.2f}"

def fmt_pct(val):
    if val is None or pd.isna(val): return "N/A"
    return f"{val*100:.2f}%"

def get_color_class(val):
    if val is None or pd.isna(val): return ""
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
            # Calculate Total Return and CAGR
            df_history['date_obj'] = pd.to_datetime(df_history['date'])
            first_date = df_history['date_obj'].min()
            last_date = df_history['date_obj'].max()
            
            latest_val = df_history.iloc[-1]['total_value']
            earliest_val = df_history.iloc[0]['total_value']
            
            total_return = (latest_val / earliest_val - 1) if earliest_val and earliest_val > 0 else 0
            
            days = (last_date - first_date).days
            years = days / 365.25
            
            if years > 0 and earliest_val > 0 and latest_val > 0:
                cagr = (latest_val / earliest_val) ** (1 / years) - 1
            else:
                cagr = 0
                
            title_str = f'Portfolio Value History (Total Return: {total_return*100:.2f}%, CAGR: {cagr*100:.2f}%)'

            # Create Plotly Chart
            fig = px.line(df_history, x='date', y=['total_value', 'total_cost_basis'], 
                          title=title_str,
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
    accum_pct = 0.0
    for _, row in df_holdings.iterrows():
        day_change_cls = get_color_class(row['day_change_dollars'])
        unrealized_cls = get_color_class(row['unrealized_gain_loss'])
        
        port_pct = row['portfolio_percentage']
        if port_pct is not None and not pd.isna(port_pct):
            accum_pct += port_pct
            
        # Clean brokers for HTML attribute, safely escape quotes
        brokers_json = "{}"
        if pd.notna(row['brokers']):
            # ensure quotes are correctly escaped for the HTML attribute
            brokers_json = str(row['brokers']).replace('"', '&quot;')
            
        # Safe numeric values for sorting
        sort_quantity = row['quantity'] if pd.notna(row['quantity']) else 0
        sort_price = row['price'] if pd.notna(row['price']) else 0
        sort_cost_basis = row['cost_basis'] if pd.notna(row['cost_basis']) else 0
        sort_current_value = row['current_value'] if pd.notna(row['current_value']) else 0
        sort_day_change = row['day_change_dollars'] if pd.notna(row['day_change_dollars']) else 0
        sort_unrealized = row['unrealized_gain_loss'] if pd.notna(row['unrealized_gain_loss']) else 0
        sort_port_pct = port_pct if pd.notna(port_pct) else 0
            
        holdings_rows += f"""
        <tr>
            <td class="fw-bold" data-sort-value="{row['symbol']}"><a href="#" class="symbol-link text-decoration-none" data-brokers="{brokers_json}" data-price="{sort_price}">{row['symbol']}</a></td>
            <td data-sort-value="{row['description']}"><span class="desc-col" title="{row['description']}">{row['description']}</span></td>
            <td class="text-end" data-sort-value="{sort_quantity}">{row['quantity']:.4f}</td>
            <td class="text-end" data-sort-value="{sort_price}">{fmt_money(row['price'])}</td>
            <td class="text-end" data-sort-value="{sort_cost_basis}">{fmt_money(row['cost_basis'])}</td>
            <td class="text-end fw-bold" data-sort-value="{sort_current_value}">{fmt_money(row['current_value'])}</td>
            <td class="text-end {day_change_cls}" data-sort-value="{sort_day_change}">
                {fmt_money(row['day_change_dollars'])}
                <br><small>{fmt_pct(row['day_change_percent'])}</small>
            </td>
            <td class="text-end {unrealized_cls}" data-sort-value="{sort_unrealized}">
                {fmt_money(row['unrealized_gain_loss'])}
                <br><small>{fmt_pct(row['unrealized_gain_loss_percent'])}</small>
            </td>
            <td class="text-end" data-sort-value="{sort_port_pct}">{fmt_pct(row['portfolio_percentage'])}</td>
            <td class="text-end" data-sort-value="{accum_pct}">{fmt_pct(accum_pct)}</td>
        </tr>
        """

    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Portfolio Report - {latest_date}</title>
    <link rel="icon" type="image/svg+xml" href="favicon.svg">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body {{ background-color: #f8f9fa; padding-bottom: 40px; }}
        .metric-card {{ transition: transform 0.2s; border: none; shadow: 0 0.5rem 1rem rgba(0, 0, 0, 0.15); }}
        .metric-label {{ font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.5px; color: #6c757d; }}
        .metric-value {{ font-size: 1.75rem; font-weight: 700; }}
        .table-holdings th {{ background-color: #f1f3f5; font-size: 0.85rem; text-transform: uppercase; color: #495057; }}
        .table-holdings td {{ vertical-align: middle; }}
        
        .desc-col {{
            max-width: 12ch;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            display: inline-block;
            vertical-align: middle;
        }}

        th.sortable {{
            cursor: pointer;
            user-select: none;
            position: relative;
            padding-right: 1.5rem !important;
        }}
        th.sortable::after {{
            content: "↕";
            position: absolute;
            right: 0.5rem;
            color: #ccc;
        }}
        th.sortable.sorted-asc::after {{
            content: "↑";
            color: #000;
        }}
        th.sortable.sorted-desc::after {{
            content: "↓";
            color: #000;
        }}
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
                <table class="table table-hover table-holdings mb-0" id="holdingsTable">
                    <thead>
                        <tr>
                            <th class="sortable" data-type="string">Symbol</th>
                            <th class="sortable" data-type="string">Description</th>
                            <th class="text-end sortable" data-type="number">Qty</th>
                            <th class="text-end sortable" data-type="number">Price</th>
                            <th class="text-end sortable" data-type="number">Base Value</th>
                            <th class="text-end sortable" data-type="number">Value</th>
                            <th class="text-end sortable" data-type="number">Day Change</th>
                            <th class="text-end sortable" data-type="number">Unrealized G/L</th>
                            <th class="text-end sortable" data-type="number">% Port</th>
                            <th class="text-end sortable" data-type="number">% Accum</th>
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

    <!-- Broker Modal -->
    <div class="modal fade" id="brokerModal" tabindex="-1" aria-hidden="true">
        <div class="modal-dialog modal-sm">
        <div class="modal-content">
            <div class="modal-header">
            <h5 class="modal-title" id="brokerModalLabel">Positions</h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body" id="brokerModalBody">
            </div>
        </div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        // Sorting logic
        document.querySelectorAll('th.sortable').forEach(th => {{
            th.addEventListener('click', () => {{
                const table = th.closest('table');
                const tbody = table.querySelector('tbody');
                const rows = Array.from(tbody.querySelectorAll('tr'));
                const index = Array.from(th.parentNode.children).indexOf(th);
                const isNumeric = th.dataset.type === 'number';
                let asc = th.dataset.sortAsc === 'true';
                
                // Clear other sorting icons
                table.querySelectorAll('th.sortable').forEach(el => {{
                    el.classList.remove('sorted-asc', 'sorted-desc');
                    if (el !== th) el.dataset.sortAsc = '';
                }});

                // Toggle sort
                asc = !asc;
                th.dataset.sortAsc = asc;
                th.classList.add(asc ? 'sorted-asc' : 'sorted-desc');
                
                // Perform sort
                rows.sort((a, b) => {{
                    let aCol = a.children[index];
                    let bCol = b.children[index];
                    
                    let aVal = aCol.dataset.sortValue !== undefined ? aCol.dataset.sortValue : aCol.innerText;
                    let bVal = bCol.dataset.sortValue !== undefined ? bCol.dataset.sortValue : bCol.innerText;
                    
                    if (isNumeric) {{
                        aVal = parseFloat(aVal) || 0;
                        bVal = parseFloat(bVal) || 0;
                        return asc ? aVal - bVal : bVal - aVal;
                    }} else {{
                        aVal = String(aVal).trim().toLowerCase();
                        bVal = String(bVal).trim().toLowerCase();
                        return asc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
                    }}
                }});
                
                tbody.append(...rows);
            }});
        }});

        // Broker Modal logic
        const brokerModalElement = document.getElementById('brokerModal');
        let brokerModal;
        if (brokerModalElement) {{
            brokerModal = new bootstrap.Modal(brokerModalElement);
        }}
        
        document.querySelectorAll('.symbol-link').forEach(link => {{
            link.addEventListener('click', (e) => {{
                e.preventDefault();
                const symbol = link.innerText;
                const brokersStr = link.dataset.brokers;
                let brokers = {{}};
                
                try {{
                    // dataset automatically unescapes HTML entities!
                    brokers = JSON.parse(brokersStr);
                }} catch(err) {{
                    console.error("Error parsing broker data:", err, brokersStr);
                }}
                
                const price = parseFloat(link.dataset.price) || 0;
                document.getElementById('brokerModalLabel').innerText = `Positions for ${{symbol}}`;
                let html = '<div class="table-responsive"><table class="table table-sm table-bordered">';
                html += '<thead><tr><th>Broker</th><th class="text-end">Units</th><th class="text-end">Value</th></tr></thead><tbody>';
                if (Object.keys(brokers).length === 0) {{
                    html += '<tr><td colspan="3" class="text-muted text-center">No broker data available.</td></tr>';
                }} else {{
                    for (const [broker, brokerValue] of Object.entries(brokers)) {{
                        const units = price > 0 ? (brokerValue / price) : 0;
                        const formattedUnits = units.toLocaleString('en-US', {{ minimumFractionDigits: 0, maximumFractionDigits: 4 }});
                        const formattedValue = new Intl.NumberFormat('en-US', {{ style: 'currency', currency: 'USD' }}).format(brokerValue);
                        html += `<tr>
                            <td>${{broker}}</td>
                            <td class="text-end">${{formattedUnits}}</td>
                            <td class="text-end">${{formattedValue}}</td>
                        </tr>`;
                    }}
                }}
                html += '</tbody></table></div>';
                document.getElementById('brokerModalBody').innerHTML = html;
                brokerModal.show();
            }});
        }});
    </script>
</body>
</html>
    """
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        f.write(html_content)
    
    print(f"Report generated successfully at {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_report()
