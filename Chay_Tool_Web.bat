@echo off
title Presto to SparkSQL Converter Tool (Web UI)
chcp 65001 > nul
cd /d "%~dp0"
echo ========================================================
echo  KHOI DONG WEB TOOL CHUYEN DOI SQL 2 CHIEU (WEB)
echo ========================================================
echo Dang mo trinh duyet tai http://localhost:7860...
python "%~dp0app_web.py"
pause
