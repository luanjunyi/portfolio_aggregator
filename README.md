# Portfolio Aggregator

A local web application that aggregates portfolio data from multiple brokers by web scraping.

## Why not Empower?

To minimize the risk of losing money.

## Features

- Scrapes portfolio data from MerrillEdge, Chase, and ETrade
- Stores encrypted sessions for minimal manual login effort
- Handles 2FA authentication flows
- Vue.js frontend for data visualization
- Local SQLite storage for credentials and sessions

## Setup

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Install Playwright browsers:
```bash
playwright install
```

3. Run the backend:
```bash
cd backend
python main.py
```

4. Open the frontend in your browser at `http://localhost:8000`

## Architecture

- **Backend**: FastAPI + Playwright for web scraping
- **Frontend**: Vue.js 3 + Tailwind CSS
- **Storage**: SQLite for credentials and encrypted sessions
- **Browser**: Playwright for JavaScript-enabled scraping
