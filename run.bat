@echo off
echo ============================================
echo   BIM Infra Solutions -- Custom CRM
echo ============================================
echo.

echo Installing dependencies...
py -m pip install flask requests python-dotenv openpyxl Werkzeug
echo.

echo Starting CRM server...
echo.
echo  Open your browser: http://localhost:5000
echo  Press Ctrl+C to stop
echo.
py app.py
pause
