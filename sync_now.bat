@echo off
title BIM CRM — Sync Now
color 0A
echo.
echo ============================================================
echo   BIM INFRA SOLUTIONS — CRM LIVE SYNC
echo ============================================================
echo.

cd /d "%~dp0"

echo [1/3] Checking database...
python -c "import sqlite3; conn=sqlite3.connect('bim_crm.db'); c=conn.cursor(); c.execute('SELECT COUNT(*) FROM leads'); n=c.fetchone()[0]; print(f'  Leads in DB: {n}'); conn.close()"

echo.
echo [2/3] Seeding + Exporting to Excel...
python seed_and_sync.py seed

echo.
echo [3/3] Checking Zoho Mail for replies...
python crm_excel_sync.py mail-sync

echo.
echo ============================================================
echo   SYNC COMPLETE — Open BIM_CRM_Sync.xlsx
echo ============================================================
echo.
pause
