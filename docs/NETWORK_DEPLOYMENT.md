# Network Deployment Guide

Complete guide for deploying the UniteUs ETL server on your network.

---

## Overview

The server can run on standard ports (80 for HTTP, 443 for HTTPS) or a custom port (8000 by default). Choose the option that best fits your needs.

---

## Deployment Options

### Port 8000 (HTTP) - Easiest Setup

**Best for:** Testing, development, quick deployment

**Access URLs:**
- `http://YOUR-HOSTNAME.calco.local:8000`
- `http://YOUR-HOSTNAME:8000`
- `http://YOUR-IP-ADDRESS:8000`

**Setup:**
1. Double-click `launch.pyw` (no admin needed)
2. Port is already set to 8000 by default
3. Share URL with users (shown in Server Control window)

**Pros:**
- ✅ No administrator privileges needed
- ✅ Works immediately
- ✅ Good for testing/development

**Cons:**
- ⚠️ Users must include `:8000` in URL
- ⚠️ HTTP is not encrypted

---

### Port 80 (HTTP) - Simple Production

**Best for:** Production environments where HTTPS is not required

**Access URLs:**
- `http://YOUR-HOSTNAME`
- `http://YOUR-HOSTNAME.calco.local`
- `http://YOUR-IP-ADDRESS`

**Setup:**
1. Right-click `launch.pyw` → **Run as administrator**
2. In Server Configuration, change port from `8000` to `80`
3. Click "Restart Server"
4. Share URL with users

**Pros:**
- ✅ Users don't need to type port number
- ✅ Simple, memorable URL
- ✅ Works with both hostname and FQDN

**Cons:**
- ⚠️ Requires administrator privileges
- ⚠️ HTTP is not encrypted (not recommended for sensitive data)

---

### Port 443 (HTTPS) - Secure Production

**Best for:** Production environments with sensitive data

**Access URLs:**
- `https://YOUR-HOSTNAME.calco.local`
- `https://YOUR-HOSTNAME`
- `https://YOUR-IP-ADDRESS`

**Setup:**
1. Right-click `launch.pyw` → **Run as administrator**
2. In Server Configuration:
   - Change port from `8000` to `443`
   - Enable HTTPS checkbox
   - For production: Click "Select Enterprise Certificate" (use County CA certificate)
   - For testing: Click "Generate Certificate" (creates self-signed with FQDN)
3. Click "Restart Server"
4. Share URL with users

**Pros:**
- ✅ Encrypted connection (secure)
- ✅ Users don't need to type port number
- ✅ County CA certificates automatically trusted on domain machines
- ✅ Professional, production-ready solution

**Cons:**
- ⚠️ Requires administrator privileges
- ⚠️ Requires certificate (self-signed for testing, County CA for production)

**For detailed HTTPS setup, see [HTTPS Setup Guide](HTTPS_SETUP.md)**

---

## Recommended Configuration

### Production Deployment
**Use Port 443 (HTTPS) with County CA Certificate**

1. **Get Certificate from County IT:**
   - Subject: `YOUR-HOSTNAME.calco.local`
   - Subject Alternative Names (SAN):
     - `YOUR-HOSTNAME.calco.local`
     - `YOUR-HOSTNAME`
     - `YOUR-IP-ADDRESS`
     - `localhost`
     - `127.0.0.1`

2. **Configure Server:**
   - Run `launch.pyw` as Administrator
   - Set port to `443`
   - Enable HTTPS
   - Select County CA certificate files

3. **Share with Users:**
   - URL: `https://YOUR-HOSTNAME.calco.local`
   - No port number needed
   - No certificate installation needed (trusted via Group Policy)

### Quick Testing
**Use Port 8000 (HTTP)**
- No admin needed
- URL: `http://YOUR-HOSTNAME.calco.local:8000`
- Works immediately

---

## User Access Instructions

**If using Port 80 or 443:**
1. Open web browser
2. Type: `http://YOUR-HOSTNAME.calco.local` (or `https://` for port 443)
3. Login with Active Directory credentials

**If using Port 8000:**
1. Open web browser
2. Type: `http://YOUR-HOSTNAME.calco.local:8000`
3. Login with Active Directory credentials

**Note:** The Server Control window shows all available URLs. Share the appropriate one with users.

---

## Windows Firewall Configuration

The server will prompt to allow access when first started. If needed, manually configure:

**GUI Method:**
1. Open **Windows Defender Firewall**
2. Click **Allow an app or feature through Windows Defender Firewall**
3. Click **Change Settings** → **Allow another app**
4. Browse to `python.exe` (or `pythonw.exe` for launch.pyw)
5. Check both **Private** and **Public** networks
6. Click **OK**

**Command Line (Admin):**
```powershell
# For port 80
netsh advfirewall firewall add rule name="UniteUs ETL Server Port 80" dir=in action=allow protocol=TCP localport=80

# For port 443
netsh advfirewall firewall add rule name="UniteUs ETL Server Port 443" dir=in action=allow protocol=TCP localport=443

# For port 8000
netsh advfirewall firewall add rule name="UniteUs ETL Server Port 8000" dir=in action=allow protocol=TCP localport=8000
```

---

## Network Requirements

**Required:**
- Same local network (WiFi/Ethernet) for users
- Server computer must be on and running `launch.pyw`
- Port must be open in Windows Firewall
- Administrator privileges for ports 80/443

**Won't work:**
- Different networks (different WiFi, cellular data)
- Across the internet (without VPN or port forwarding)
- If server computer is asleep/off

---

## Getting Your Network Information

The Server Control window automatically displays:
- **Local URL:** For access from the server computer only
- **Network IP:** For access from other computers on the network
- **Hostname:** Alternative access method

These URLs are automatically generated based on your network configuration.

---

**For quick setup, see [Quick Setup Guide](QUICK_SETUP.md)**  
**For HTTPS configuration, see [HTTPS Setup Guide](HTTPS_SETUP.md)**  
**For troubleshooting, see [Troubleshooting Guide](TROUBLESHOOTING.md)**

