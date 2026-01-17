# 进入项目目录
Set-Location -Path "d:\Codes\GitHub\BinanceData"

# 激活conda环境
conda activate stock

# 以开发者模式安装包
pip install -e .

Write-Host "安装完成！" -ForegroundColor Green
Read-Host "按Enter键退出"