@echo off
rem Task Scheduler discards stderr: send both streams to a log file. Exits with npd-loader's exit code.
if not exist "C:\npd-loader\logs" mkdir "C:\npd-loader\logs"
"%~dp0..\.venv\Scripts\npd-loader.exe" --config C:\npd-loader\config.toml run >> C:\npd-loader\logs\npd-loader.log 2>&1
exit /b %ERRORLEVEL%
