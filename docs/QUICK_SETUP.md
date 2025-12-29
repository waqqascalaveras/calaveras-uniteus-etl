# Quick Setup Guide

Get the UniteUs ETL server running in minutes.

---

## For Server Administrators

### Step 1: Launch the Server
1. Double-click `launch.pyw`
2. When Windows Firewall prompts, click **"Allow access"**

### Step 2: Get the Network URL
The Server Control window displays three URLs:
- **Local URL:** For access from this computer only
- **Network IP:** Share this with users on your network
- **Hostname:** Alternative access method

### Step 3: Share with Users
- Copy the **Network IP** URL from the Server Control window
- Send to users via email, Teams, or other method
- Users must be on the same network (same WiFi/Ethernet)

---

## For End Users

### Step 1: Get the URL
Ask your server administrator for the Network IP URL  
Example: `http://192.168.1.50:8000`

### Step 2: Open in Browser
1. Open Chrome, Edge, or Firefox
2. Type or paste the URL
3. Press Enter

### Step 3: Login
- Use your Windows credentials (Active Directory)
- Or use local username/password if provided

---

## Common Issues

### "Can't reach this page"
- Verify you're on the same network as the server
- Try pinging the server: `ping 192.168.1.50`
- Ask admin to verify server is running

### Connection timeout
- Server might be down
- Firewall might be blocking
- Try the hostname URL instead

### "403 Forbidden" or login fails
- Check username/password
- Contact administrator for access

---

## Windows Firewall Setup

If you missed the firewall prompt:

**PowerShell (Admin):**
```powershell
New-NetFirewallRule -DisplayName "UniteUs ETL Server" -Direction Inbound -Protocol TCP -LocalPort 8000 -Action Allow
```

**GUI:**
1. Control Panel → Windows Defender Firewall
2. Advanced settings → Inbound Rules → New Rule
3. Port → TCP → 8000 → Allow → All profiles → Finish

---

## Network Requirements

**Required:**
- Same local network (WiFi/Ethernet)
- Server computer must be running `launch.pyw`
- Port must be open in Windows Firewall

**Won't work:**
- Different networks (different WiFi, cellular data)
- Across the internet (without VPN)
- If server computer is asleep/off

---

## Quick Test

From a user's computer (PowerShell):
```powershell
# Test connectivity
ping 192.168.1.50

# Test port access
Test-NetConnection -ComputerName 192.168.1.50 -Port 8000
```

Expected: `TcpTestSucceeded : True`

---

**For production deployment options (Port 80, 443, HTTPS), see [Network Deployment Guide](NETWORK_DEPLOYMENT.md)**

