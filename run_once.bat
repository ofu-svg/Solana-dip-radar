@echo off
cd /d "%~dp0"
call .venv\Scripts\activate
python solana_dip_radar.py
pause
