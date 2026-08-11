# Debugging a failed daily crawl

How to figure out why the daily portfolio crawl failed, and how to recover.

## How the daily job works

- **Scheduler:** a macOS LaunchAgent (`com.jluan.portfolio.daily`, defined in
  `~/code/worker_master/workers/com.jluan.portfolio.daily.plist`) runs every day
  at **14:00 local**. It calls the worker wrapper, which runs
  `backend/run_daily_portfolio.py` with cwd `~/code/portfolio`.
- **Pipeline:** `fetch_all_positions()` runs the three broker crawlers in order
  — **Merrill → Chase → E\*Trade** — each in its own headful Chrome over CDP.
- **Retry:** each broker is attempted up to **3 times** with a 10s backoff
  (`CRAWLER_MAX_ATTEMPTS` / `CRAWLER_RETRY_BACKOFF_SECONDS` in
  `backend/fetch_all_positions.py`). A fresh browser launches each attempt.
- **All-or-nothing:** if any broker still fails after 3 attempts, the **whole
  run aborts and nothing is saved**. Partial snapshots are intentionally not
  written. So a missing day means at least one broker failed all retries.
- On success it backs up the DB (`backups/`) then writes one
  `portfolio_snapshots` row + per-holding `holdings_snapshots` rows for the
  local date.

## Where the failure info lives

Failure detail is **kept even when a retry later succeeds** — each failed
attempt is logged, and E\*Trade also saves a page snapshot per failed attempt.

| What | Where | Notes |
|---|---|---|
| Run logs (primary) | `~/Library/Logs/workers/portfolio_daily/current.log` | Worker captures stdout. Appends across runs with `--- Starting / Exited ---` markers. This is the authoritative log for **scheduled** runs. |
| Run logs (repo copy) | `~/code/portfolio/portfolio_cron.log` | Same content via a `FileHandler`. |
| E\*Trade page snapshots | `~/code/portfolio/etrade_debug/etrade_<tag>_<ts>.png` and `.html` | Screenshot + HTML + (logged) URL/title at the moment E\*Trade login failed. Only E\*Trade; only on attempts that reached the login form / positions stage. Persists on disk even if a later attempt succeeds. |

Both log files are gitignored; `etrade_debug/` is gitignored too.

> Note: a transient failure that was auto-recovered still leaves WARNING lines.
> `grep WARNING` to see flakiness even on days the job ultimately succeeded.

## Step-by-step

### 1. Find the most recent run

```bash
grep -nE "Starting portfolio_daily|Exited with code" \
  ~/Library/Logs/workers/portfolio_daily/current.log | tail -4
```

Exit code `0` = success, `1` = failed. Note the start line number to scope the
rest of your search to that run (`tail -n +<line>`).

### 2. See which broker failed and why

```bash
grep -aE "Successfully scraped|attempt [0-9]/[0-9]|failed after|Saved E\*TRADE|Failed to run daily|Error running crawler" \
  ~/Library/Logs/workers/portfolio_daily/current.log | tail -30
```

You're looking for:
- `[broker] Successfully scraped N holdings` — that broker is fine.
- `<Crawler> attempt 1/3 raised: ...` — a retried failure (with traceback
  immediately below it in the log).
- `<Crawler> failed after 3 attempts; last error: ... Manual retry/fix needed.`
  — the broker that killed the run.

### 3. For E\*Trade, look at the captured page

E\*Trade is the usual culprit (intermittent login stalls). When it fails it
logs a line like `Saved E*TRADE login debug [<tag>]: url=... title=... -> <path>`.
Open the artifact to see what wall it hit:

```bash
ls -lt ~/code/portfolio/etrade_debug/ | head
open ~/code/portfolio/etrade_debug/etrade_after_login_click_<ts>.png
```

Interpreting the screenshot/title:
- An **MFA / security-code / "verify it's you"** page → E\*Trade is challenging
  the unattended session; it can't be cleared without a human. Do a manual run
  to re-establish a trusted session (see Recovery).
- A **CAPTCHA / "press and hold" / access-denied** page → bot detection / IP
  block. Manual run; consider spacing out runs.
- The **normal login form** with no progress → navigation stalled. The explicit
  90s timeouts now cap this (it used to hang ~15 min because `page.goto` over the
  CDP-attached browser had no enforced timeout).

`tag` meanings: `no_login_form` = login form never loaded after navigation;
`after_login_click` = credentials submitted but never reached the positions page.

## Recovery

### Manual re-run (interactive — usually succeeds)

```bash
cd ~/code/portfolio
/opt/anaconda3/envs/ai/bin/python backend/run_daily_portfolio.py
```

Running interactively typically clears the E\*Trade challenge (you're present to
complete any prompt, and the session becomes trusted). Add `--always` to force a
run on a non-trading day. A successful run writes today's snapshot.

### Re-run under the real scheduler (to reproduce the launchd context)

```bash
launchctl start com.jluan.portfolio.daily
# then watch:
tail -f ~/Library/Logs/workers/portfolio_daily/current.log
```

### Fixing a wrong snapshot date

The snapshot date comes from local time (`datetime.now()`), matching the
`date.today()` trading-day check. If you ever need to relabel a row, update both
tables (and back up first via `backend/storage/backup.py`):

```sql
UPDATE portfolio_snapshots SET date='YYYY-MM-DD' WHERE date='<wrong>';
UPDATE holdings_snapshots  SET date='YYYY-MM-DD' WHERE date='<wrong>';
```

## Key code locations

- `backend/run_daily_portfolio.py` — entry point, trading-day gate, logging,
  DB backup + save.
- `backend/fetch_all_positions.py` — crawler orchestration, **retry logic**
  (`_run_crawler_with_retries`), combine, abort-on-failure.
- `backend/crawlers/etrade_crawler.py` — E\*Trade login, timeouts, and
  `_save_login_debug()` (the screenshot/HTML capture).
- `backend/crawlers/base_crawler.py` — Chrome launch over CDP, session handling.
- `~/code/worker_master/workers/portfolio_daily.py` + `.plist` — the scheduled
  wrapper and its 14:00 trigger.
