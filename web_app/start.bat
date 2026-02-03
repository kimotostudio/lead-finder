@echo off
echo Starting Lead Finder Web Application...
echo.

REM Check if virtual environment exists
if not exist venv (
    echo Creating virtual environment...
    python -m venv venv
)

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Install dependencies
echo Installing dependencies...
pip install -r requirements.txt --quiet

REM Create necessary directories
if not exist output mkdir output
if not exist logs mkdir logs

REM Start Flask app
echo.
echo ============================================
echo Lead Finder Web App is starting...
echo Open your browser and go to:
echo.
echo     http://localhost:5000
echo.
echo Press Ctrl+C to stop the server
echo ============================================
echo.

python app.py
