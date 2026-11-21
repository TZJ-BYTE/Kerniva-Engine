@echo off
setlocal
chcp 65001 >nul
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0kerniva.ps1" %*
exit /b %errorlevel%
