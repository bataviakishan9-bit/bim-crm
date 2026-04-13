@echo off
title BIM CRM — Auto Sync (Every 5 Min)
color 0B
echo.
echo ============================================================
echo   BIM CRM — AUTO SYNC MODE (every 5 minutes)
echo   Press Ctrl+C to stop
echo ============================================================
echo.

cd /d "%~dp0"

REM First run: seed + export
echo [STARTUP] Seeding database + generating Excel...
python seed_and_sync.py seed
echo.
echo [STARTUP] Checking Zoho Mail for replies...
python crm_excel_sync.py mail-sync

echo.
echo Starting watch loop...
echo.

:LOOP
echo [%TIME%] Running full sync...
python crm_excel_sync.py export
python crm_excel_sync.py mail-sync
echo [%TIME%] Sync done. Next sync in 5 minutes...
echo.
timeout /t 300 /nobreak
goto LOOP
