# HTTPS Setup Guide

Complete guide for setting up HTTPS with FQDN (Fully Qualified Domain Name) support.

---

## Overview

For `https://YOUR-HOSTNAME.calco.local` to work, the SSL certificate **must** include the FQDN in the **Subject Alternative Names (SAN)** field. The application automatically handles this when generating self-signed certificates.

---

## The Problem

When a browser connects to `https://YOUR-HOSTNAME.calco.local`, it checks:
1. Does the certificate match the hostname in the URL? ✅
2. Is the certificate trusted? ✅ (if using County CA)

If the certificate only has the hostname but not the FQDN, the browser will show a certificate mismatch error.

---

## Solution Options

### Option 1: Self-Signed Certificate (Testing/Development)

**The application automatically includes FQDN when generating certificates.**

**Steps:**
1. Run `launch.pyw` as Administrator
2. In the Server Control window:
   - Set port to `443`
   - Enable the "Enable HTTPS" checkbox
   - Click "Generate Certificate" (if certificate doesn't exist)
3. Click "Restart Server"

**What gets automatically included in the certificate:**
- `localhost`
- `127.0.0.1`
- Hostname (e.g., `YOUR-HOSTNAME`)
- Hostname lowercase
- FQDN (e.g., `YOUR-HOSTNAME.calco.local`) ✅
- FQDN lowercase ✅
- IP address

**Result:** `https://YOUR-HOSTNAME.calco.local` will work (after users install the certificate)

**User Certificate Installation:**
- Access `https://YOUR-HOSTNAME.calco.local`
- Click "Advanced" → "Proceed to site" (or install certificate)
- Or use the "Install Certificate" button in the Server Control GUI

---

### Option 2: County CA Certificate (Production - Recommended)

**For production**, request a certificate from Calaveras County's Certificate Authority.

**Certificate Request Requirements:**

When requesting the certificate from County IT, specify:

1. **Subject (Common Name):**
   - `YOUR-HOSTNAME.calco.local` (use FQDN as CN)

2. **Subject Alternative Names (SAN) - REQUIRED:**
   ```
   - YOUR-HOSTNAME.calco.local (FQDN - primary)
   - YOUR-HOSTNAME (hostname)
   - YOUR-IP-ADDRESS (IP address)
   - localhost
   - 127.0.0.1
   ```

3. **Certificate Details:**
   - Key Size: 2048-bit minimum (4096-bit recommended)
   - Format: PEM (.crt/.pem for certificate, .key/.pem for private key)
   - Validity: Per County policy (typically 1-3 years)
   - Key Usage: Digital Signature, Key Encipherment
   - Extended Key Usage: Server Authentication

**After receiving the certificate:**

1. Copy certificate and key files to the server
2. In `launch.pyw` GUI:
   - Click "Select Enterprise Certificate"
   - Select the certificate file (.crt or .pem)
   - Select the private key file (.key or .pem)
   - Restart server

**Result:** `https://YOUR-HOSTNAME.calco.local` will work automatically on all domain machines (no certificate installation needed)

---

## How It Works

### Certificate Matching Process

When a user accesses `https://YOUR-HOSTNAME.calco.local`:

1. **Browser checks:** Does the certificate match the hostname in the URL?
   - Looks for `YOUR-HOSTNAME.calco.local` in the certificate's Subject Alternative Names
   - ✅ **Found!** (automatically included in self-signed certs)

2. **Browser checks:** Is the certificate trusted?
   - Self-signed: Users need to install it (one-time)
   - County CA: Automatically trusted on domain machines

3. **Result:** Connection succeeds ✅

### Why Subject Alternative Names (SAN)?

Modern browsers require the exact hostname in the URL to match one of the SAN entries. The Common Name (CN) is not enough for hostname validation.

**Certificate Structure:**
```
Common Name: YOUR-HOSTNAME
Subject Alternative Names:
  - YOUR-HOSTNAME.calco.local  ← Required for FQDN access
  - YOUR-HOSTNAME              ← For hostname access
  - YOUR-IP-ADDRESS            ← For IP access
  - localhost                 ← For local access
```

### Certificate Matching Rules

| Access Method | Certificate Requirement | Status |
|--------------|------------------------|--------|
| `https://YOUR-HOSTNAME.calco.local` | FQDN in SAN | ✅ Auto-included |
| `https://YOUR-HOSTNAME` | Hostname in SAN | ✅ Auto-included |
| `https://YOUR-IP-ADDRESS` | IP in SAN | ✅ Auto-included |

---

## Verification

The application automatically verifies certificates include the FQDN. You can also check manually:

**Check certificate details in the Server Control GUI:**
- Look at the "Security & Certificates" section
- Certificate status shows what names are included

**Expected result:**
- ✅ Certificate includes all required names
- ✅ HTTPS should work with FQDN

---

## Setup Steps Summary

### For Self-Signed Certificate (Testing)

1. ✅ Certificate generation automatically includes FQDN
2. Run `launch.pyw` as Administrator
3. Set port to `443`
4. Enable HTTPS checkbox
5. Click "Restart Server"
6. Users install certificate (one-time per machine)
7. ✅ `https://YOUR-HOSTNAME.calco.local` works

### For County CA Certificate (Production)

1. Request certificate from County IT with FQDN in SAN
2. Copy certificate and key files to server
3. In GUI: Click "Select Enterprise Certificate"
4. Select certificate and key files
5. Restart server
6. ✅ `https://YOUR-HOSTNAME.calco.local` works automatically

---

## Troubleshooting

### Certificate Mismatch Error

**Symptom:** Browser shows "Certificate does not match hostname"

**Solution:**
- Verify certificate includes FQDN in SAN (check in GUI)
- Regenerate certificate if needed (will auto-include FQDN)
- For County CA: Request new certificate with FQDN in SAN

### Certificate Not Trusted

**Symptom:** Browser shows "Certificate is not trusted"

**Solution:**
- **Self-signed:** Users must install certificate (one-time)
- **County CA:** Verify certificate is from County CA and properly installed

### HTTPS Not Working

**Symptom:** Cannot connect via HTTPS

**Check:**
1. Server is running on port 443
2. HTTPS checkbox is enabled
3. Certificate files exist in `data/ssl/` or enterprise cert is selected
4. Windows Firewall allows port 443
5. Running as Administrator (required for port 443)

---

## Technical Details

### Automatic FQDN Detection

The application automatically detects the FQDN using:
1. `socket.getfqdn()` - Python's built-in FQDN detection
2. Windows `net config` command - Gets domain information
3. DNS suffix from network adapter settings
4. Fallback: Constructs FQDN from hostname and DNS suffix

### Certificate Generation

When generating self-signed certificates, the application:
- Automatically detects FQDN
- Includes FQDN in Subject Alternative Names
- Includes hostname, IP address, and localhost variants
- Logs all included names for verification

---

## Summary

**Question:** Do we need to manually configure FQDN in certificates?

**Answer:**
- ✅ **No** - The application automatically includes FQDN when generating self-signed certificates
- ✅ **Yes** - For County CA certificates, you must request FQDN in SAN from County IT
- ✅ **Ready** - `https://YOUR-HOSTNAME.calco.local` will work once HTTPS is enabled and certificate is installed

**No additional configuration needed for self-signed certificates** - The application handles FQDN automatically.

---

**For network deployment options, see [Network Deployment Guide](NETWORK_DEPLOYMENT.md)**
