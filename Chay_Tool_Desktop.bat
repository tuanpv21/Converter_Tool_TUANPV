@echo off
title Presto to SparkSQL Converter Tool (Desktop)
chcp 65001 > nul
cd /d "%~dp0"
echo ========================================================
echo  KHOI DONG TOOL CHUYEN DOI SQL 2 CHIEU (DESKTOP)
echo ========================================================
echo Dang mo giao dien Desktop...
start pythonw "%~dp0app_gui.py"
exit
