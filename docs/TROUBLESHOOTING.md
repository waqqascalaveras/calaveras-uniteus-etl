# Troubleshooting Guide

Solutions for common issues when deploying and using the UniteUs ETL server.

---

## Server Won't Start

### Port Already in Use

**Symptom:** Error message about port being in use

**Solution:**
1. Check what's using the port:
   ```powershell
   netstat -ano | findstr ":8000"
   ```
2. Change to a different port in Server Configuration
3. Or stop the service using the port

### Administrator Privileges Required

**Symptom:** Cannot bind to port 80 or 443

**Solution:**
- Right-click `launch.pyw` → **Run as administrator**
- Ports 80 and 443 require administrator privileges

---

## Users Can't Access Server

### Firewall Blocking

**Symptom:** Works on localhost but not from other machines

**Solution:**
1. Check Windows Firewall rules:
   ```powershell
   Get-NetFirewallRule | Where-Object {$_.DisplayName -like "*UniteUs*"}
   ```
2. If rule doesn't exist, add it:
   ```powershell
   netsh advfirewall firewall add rule name="UniteUs ETL Server" dir=in action=allow protocol=TCP localport=8000
   ```
3. Replace `8000` with your port number

### Wrong Network

**Symptom:** Connection timeout from other computers

**Solution:**
- Verify users are on the same network (same WiFi/Ethernet)
- Try IP address instead of hostname: `http://YOUR-IP-ADDRESS:8000`
- Check if server computer is awake and running

### DNS Not Resolving

**Symptom:** Hostname doesn't work, IP address does

**Solution:**
- Use IP address: `http://YOUR-IP-ADDRESS:8000`
- Or use FQDN: `http://YOUR-HOSTNAME.calco.local:8000`
- Check Server Control window for correct URLs

---

## Browser Issues

### Browser Auto-Upgrading HTTP to HTTPS

**Symptom:** Browser shows `https://` even though you typed `http://`

**Solution:**
1. **Type the URL explicitly with `http://`:**
   ```
   http://localhost
   http://YOUR-IP-ADDRESS
   http://YOUR-HOSTNAME.calco.local
   ```

2. **Clear HSTS (HTTP Strict Transport Security) settings:**
   - **Chrome/Edge:** Go to `chrome://net-internals/#hsts`
   - Scroll to "Delete domain security policies"
   - Enter: `localhost`, `YOUR-IP-ADDRESS`, `YOUR-HOSTNAME.calco.local`
   - Click "Delete"

3. **Try a different browser** (Firefox, Edge, Chrome)

### Browser Cache

**Symptom:** Old error page appears, changes don't reflect

**Solution:**
1. Press `Ctrl+Shift+Delete` to clear cache
2. Or use Incognito/Private mode
3. Hard refresh: `Ctrl+F5`

### Wrong URL Format

**Common Mistakes:**
- ❌ `https://localhost` (HTTPS not enabled on port 80)
- ❌ `localhost:80` (missing http://)
- ❌ `http://localhost:443` (wrong port)

**Correct URLs:**
- ✅ `http://localhost`
- ✅ `http://localhost:80`
- ✅ `http://YOUR-IP-ADDRESS` (use Network IP from Server Control window)
- ✅ `http://YOUR-HOSTNAME.calco.local` (use Hostname from Server Control window)

---

## HTTPS Issues

### Certificate Mismatch Error

**Symptom:** Browser shows "Certificate does not match hostname"

**Solution:**
- Verify certificate includes FQDN in SAN (check in Server Control GUI)
- Regenerate certificate if needed (will auto-include FQDN)
- For County CA: Request new certificate with FQDN in SAN

**See [HTTPS Setup Guide](HTTPS_SETUP.md) for details**

### Certificate Not Trusted

**Symptom:** Browser shows "Certificate is not trusted"

**Solution:**
- **Self-signed:** Users must install certificate (one-time)
- **County CA:** Verify certificate is from County CA and properly installed

### HTTPS Not Working

**Symptom:** Cannot connect via HTTPS

**Check:**
1. Server is running on port 443
2. HTTPS checkbox is enabled in Server Configuration
3. Certificate files exist in `data/ssl/` or enterprise cert is selected
4. Windows Firewall allows port 443
5. Running as Administrator (required for port 443)

---

## Authentication Issues

### Login Fails

**Symptom:** "403 Forbidden" or login fails

**Solution:**
- Check username/password
- Verify Active Directory connectivity (if using AD)
- Check user permissions in admin panel
- Contact administrator for access

### Session Expired

**Symptom:** Logged out unexpectedly

**Solution:**
- Sessions timeout after 1 hour of inactivity
- Simply log in again

---

## Quick Diagnostic Steps

### Test Server Connectivity

**From command line:**
```powershell
# Test HTTP response
Invoke-WebRequest -Uri "http://localhost:8000" -UseBasicParsing

# Should return: StatusCode: 200
```

### Check Server Status

1. Look for "Calaveras UniteUs ETL Server Control" window
2. Status should show "Running" (green circle)
3. Check Server Control window for current URLs

### Check Port Listening

```powershell
# Check if port is listening
netstat -ano | findstr ":8000.*LISTENING"

# Should show: TCP    0.0.0.0:8000             0.0.0.0:0              LISTENING
```

### Check Browser Console

1. Open Developer Tools (F12)
2. Go to Network tab
3. Check "Disable cache"
4. Try accessing the URL
5. Look at the request - should show Status 200
6. Check Console tab for error messages

---

## Network Testing

### Test Basic Connectivity

```powershell
# Ping the server
ping YOUR-IP-ADDRESS

# Test if port is open
Test-NetConnection -ComputerName YOUR-IP-ADDRESS -Port 8000
```

**Expected result:** `TcpTestSucceeded : True`

### Test from Another Computer

1. Ensure both computers are on the same network
2. Try accessing the server from another computer
3. Use the Network IP URL from Server Control window

---

## Still Not Working?

1. **Check server logs** - Located in `data/logs/etl_*.log`
2. **Verify server is running** - Check Server Control window
3. **Check Windows Firewall** - Ensure port is allowed
4. **Try different port** - Use port 8080 or 8443 as alternative
5. **Check network connectivity** - Ensure users are on same network

---

## Alternative Solutions

### Use Different Port

If port 80 or 443 continues to have issues:
1. Change port in Server Configuration to `8080` (HTTP) or `8443` (HTTPS)
2. Access via: `http://YOUR-HOSTNAME:8080`
3. No admin privileges needed
4. No port conflicts

### Use IP Address Instead of Hostname

If DNS is not working:
- Use IP address: `http://YOUR-IP-ADDRESS:8000`
- IP address is always available from Server Control window

---

**For network deployment options, see [Network Deployment Guide](NETWORK_DEPLOYMENT.md)**  
**For HTTPS setup, see [HTTPS Setup Guide](HTTPS_SETUP.md)**

