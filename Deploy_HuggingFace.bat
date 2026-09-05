@echo off
title Deploy Presto-Spark Transpiler to Hugging Face Spaces
chcp 65001 > nul
cd /d "%~dp0"

echo ====================================================================
echo  HUONG DAN VA AUTO DEPLOY LEN HUGGING FACE SPACES (MIEN PHI 100%%)
echo ====================================================================
echo.
echo B1: Vao https://huggingface.co/new-space
echo     - Dat ten Space (VD: sql-transpiler)
echo     - Chon SDK: Docker (Blank)
echo     - Chon: Public hoac Private tuy y
echo     - Bam "Create Space"
echo.
echo B2: Copy duong link Git Clone cua Space vua tao
echo     (Dang: https://huggingface.co/spaces/YOUR_USERNAME/sql-transpiler)
echo.
echo ====================================================================
set /p SPACE_URL="Nhap duong link Git Space cua ban roi nhan Enter: "

if "%SPACE_URL%"=="" (
    echo [Loi] Ban chua nhap link Space!
    pause
    exit /b
)

echo.
echo [1/3] Khoi tao Git repository...
if not exist ".git" (
    git init
    git branch -M main
)

echo [2/3] Them file va tao commit...
git add Dockerfile requirements.txt convert_presto_to_spark.py app_web.py README.md .dockerignore .gitignore
git commit -m "Deploy SQL Transpiler to Hugging Face Spaces"

echo [3/3] Dang day code len Hugging Face...
git remote remove hf 2>nul
git remote add hf %SPACE_URL%
git push -u hf main --force

echo.
echo ====================================================================
echo  HOAN TAT! Hugging Face se build container trong 30s.
echo  Hay mo lai trang Space tren trinh duyet de xem ket qua.
echo ====================================================================
pause
