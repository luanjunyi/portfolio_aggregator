#!/bin/bash
# Navigate to the project directory
cd /Users/jluan/code/portfolio

# Activate environment variables if needed (e.g. for browser drivers)
# export PATH=$PATH:/usr/local/bin

# Run the python script using the specific python environment
/opt/anaconda3/envs/ai/bin/python backend/run_daily_portfolio.py "$@"

# Generate the frontend report
/opt/anaconda3/envs/ai/bin/python backend/generate_report.py
