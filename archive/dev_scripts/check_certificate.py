#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Certificate Analysis Script - Check if certificate supports FQDN
"""

import socket
from pathlib import Path
import sys

def get_fqdn():
    """Get FQDN"""
    hostname = socket.gethostname()
    fqdn = socket.getfqdn()
    return hostname, fqdn if fqdn != hostname else None

def check_certificate(cert_file: Path):
    """Check certificate details"""
    try:
        from cryptography import x509
        from cryptography.hazmat.backends import default_backend
        
        with open(cert_file, 'rb') as f:
            cert_data = f.read()
        
        cert = x509.load_pem_x509_certificate(cert_data, default_backend())
        
        # Get Common Name
        cn = None
        for attr in cert.subject:
            if attr.oid._name == 'commonName':
                cn = attr.value
                break
        
        # Get Subject Alternative Names
        sans = []
        try:
            san_ext = cert.extensions.get_extension_for_oid(x509.oid.ExtensionOID.SUBJECT_ALTERNATIVE_NAME)
            for name in san_ext.value:
                if isinstance(name, x509.DNSName):
                    sans.append(name.value)
                elif isinstance(name, x509.IPAddress):
                    sans.append(str(name.value))
        except:
            pass
        
        return {
            'common_name': cn,
            'subject_alternative_names': sans,
            'valid': True
        }
    except Exception as e:
        return {
            'error': str(e),
            'valid': False
        }

def main():
    print("=" * 70)
    print("Certificate FQDN Compatibility Check")
    print("=" * 70)
    print()
    
    hostname, fqdn = get_fqdn()
    print(f"Hostname: {hostname}")
    print(f"FQDN: {fqdn}")
    print()
    
    # Check default certificate location
    cert_file = Path("data/ssl/server.crt")
    
    if not cert_file.exists():
        print(f"❌ Certificate not found: {cert_file}")
        print()
        print("ISSUE: Certificate doesn't exist yet.")
        print("SOLUTION: Generate certificate with FQDN support (see below)")
        return
    
    print(f"Checking certificate: {cert_file}")
    result = check_certificate(cert_file)
    
    if not result.get('valid'):
        print(f"❌ Error reading certificate: {result.get('error')}")
        return
    
    print(f"Common Name: {result.get('common_name')}")
    print(f"Subject Alternative Names (SAN):")
    for san in result.get('subject_alternative_names', []):
        print(f"  - {san}")
    print()
    
    # Check if FQDN is supported
    required_names = [
        hostname,
        hostname.lower(),
        fqdn if fqdn else None,
        f"{hostname}.calco.local" if fqdn else None
    ]
    required_names = [n for n in required_names if n]
    
    sans = result.get('subject_alternative_names', [])
    missing = []
    for name in required_names:
        if name not in sans:
            missing.append(name)
    
    if missing:
        print("❌ ISSUE: Certificate is missing required names:")
        for name in missing:
            print(f"   - {name}")
        print()
        print("SOLUTION:")
        print("1. Delete existing certificate:")
        print(f"   - {cert_file}")
        print(f"   - {cert_file.parent / 'server.key'}")
        print("2. Update launch.pyw to include FQDN in certificate generation")
        print("3. Regenerate certificate")
    else:
        print("✅ Certificate includes all required names!")
        print("✅ HTTPS should work with FQDN")

if __name__ == '__main__':
    main()

