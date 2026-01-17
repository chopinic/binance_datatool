@echo off

REM 进入项目目录
cd /d "d:\Codes\GitHub\BinanceData"

REM 激活conda环境
call conda activate stock

REM 以开发者模式安装包
pip install -e .

echo 安装完成！
pause