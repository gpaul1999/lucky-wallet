@echo off
chcp 65001 >nul

set RPC_URLS=https://cloudflare-eth.com/v1/mainnet,https://eth.drpc.org/,https://ethereum.publicnode.com/
set CONCURRENCY=30
set SAVE_FILE=found_keys.txt

echo ==========================================
echo   Lucky Wallet Scanner
echo ==========================================
echo RPC(s)     : %RPC_URLS%
echo Workers    : %CONCURRENCY%
echo Save file  : %SAVE_FILE%
echo ==========================================
echo.

.\target\release\lucky-wallet.exe

pause
