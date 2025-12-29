@echo off
REM ============================================================================
REM Calaveras UniteUs ETL - Quick Network Analysis (Batch Script)
REM ============================================================================
REM Quick Windows network analysis for deployment planning
REM ============================================================================

echo.
echo ========================================================================
echo Calaveras UniteUs ETL - Network Analysis
echo ========================================================================
echo.

echo [1] Hostname and Computer Information
echo ------------------------------------------------------------------------
echo Computer Name:
net config workstation | findstr "Computer name"
echo.
echo Full Computer Name:
net config workstation | findstr "Full Computer name"
echo.
echo Workstation Domain:
net config workstation | findstr "Workstation domain"
echo.

echo [2] Network Configuration
echo ------------------------------------------------------------------------
ipconfig /all | findstr /C:"IPv4" /C:"Subnet" /C:"Gateway" /C:"DNS" /C:"adapter"
echo.

echo [3] DNS Resolution Test
echo ------------------------------------------------------------------------
for /f "tokens=1" %%i in ('hostname') do set HOSTNAME=%%i
echo Testing hostname: %HOSTNAME%
ping -n 1 %HOSTNAME% >nul 2>&1
if %errorlevel%==0 (
    echo [OK] Hostname resolves
) else (
    echo [FAIL] Hostname does not resolve
)
echo.

echo [4] Port Availability Check
echo ------------------------------------------------------------------------
echo Checking port 80...
netstat -ano | findstr ":80 " | findstr "LISTENING"
if %errorlevel%==0 (
    echo [IN USE] Port 80 is in use
) else (
    echo [AVAILABLE] Port 80 is available
)
echo.
echo Checking port 443...
netstat -ano | findstr ":443 " | findstr "LISTENING"
if %errorlevel%==0 (
    echo [IN USE] Port 443 is in use
) else (
    echo [AVAILABLE] Port 443 is available
)
echo.
echo Checking port 8000...
netstat -ano | findstr ":8000 " | findstr "LISTENING"
if %errorlevel%==0 (
    echo [IN USE] Port 8000 is in use
) else (
    echo [AVAILABLE] Port 8000 is available
)
echo.

echo [5] Administrator Privileges
echo ------------------------------------------------------------------------
net session >nul 2>&1
if %errorlevel%==0 (
    echo [OK] Running with administrator privileges
    echo [OK] Can use ports 80 and 443
) else (
    echo [WARNING] Not running as administrator
    echo [WARNING] Cannot use ports 80 or 443
)
echo.

echo [6] Windows Firewall Status
echo ------------------------------------------------------------------------
netsh advfirewall show allprofiles state | findstr "State"
echo.

echo ========================================================================
echo Analysis complete!
echo ========================================================================
echo.
echo For detailed analysis with recommendations, run: python analyze_network.py
echo.
pause

