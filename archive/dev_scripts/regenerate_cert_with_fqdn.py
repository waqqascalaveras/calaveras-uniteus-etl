#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Regenerate SSL Certificate with FQDN Support
This script regenerates the certificate to include the FQDN in Subject Alternative Names
"""

import socket
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
import ipaddress

def get_network_info():
    """Get hostname, IP, and FQDN"""
    hostname = socket.gethostname()
    try:
        local_ip = socket.gethostbyname(hostname)
    except:
        local_ip = "127.0.0.1"
    
    # Get FQDN
    fqdn = socket.getfqdn()
    if fqdn == hostname:
        # Try to get from Windows
        try:
            result = subprocess.run(['net', 'config', 'workstation'], 
                                  capture_output=True, text=True, timeout=5)
            for line in result.stdout.split('\n'):
                if 'Full Computer name' in line:
                    full_name = line.split('Full Computer name')[1].strip()
                    if '.' in full_name:
                        fqdn = full_name
                        break
        except:
            # Try DNS suffix
            try:
                result = subprocess.run(['ipconfig', '/all'], 
                                      capture_output=True, text=True, timeout=5)
                for line in result.stdout.split('\n'):
                    if 'Primary Dns Suffix' in line:
                        suffix = line.split(':')[1].strip() if ':' in line else None
                        if suffix:
                            fqdn = f"{hostname}.{suffix}"
                            break
            except:
                pass
    
    return hostname, local_ip, fqdn

def regenerate_certificate():
    """Regenerate certificate with FQDN support"""
    try:
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization
    except ImportError:
        print("❌ Error: cryptography package not installed")
        print("   Install with: pip install cryptography")
        return False
    
    hostname, local_ip, fqdn = get_network_info()
    
    print("=" * 70)
    print("Regenerating SSL Certificate with FQDN Support")
    print("=" * 70)
    print()
    print(f"Hostname: {hostname}")
    print(f"FQDN: {fqdn}")
    print(f"IP Address: {local_ip}")
    print()
    
    # Certificate file paths
    cert_file = Path("data/ssl/server.crt")
    key_file = Path("data/ssl/server.key")
    
    # Backup existing certificate
    if cert_file.exists():
        backup_cert = cert_file.with_suffix('.crt.backup')
        backup_key = key_file.with_suffix('.key.backup')
        print(f"Backing up existing certificate...")
        if backup_cert.exists():
            backup_cert.unlink()
        if backup_key.exists():
            backup_key.unlink()
        cert_file.rename(backup_cert)
        if key_file.exists():
            key_file.rename(backup_key)
        print(f"  ✓ Backed up to: {backup_cert}")
        print(f"  ✓ Backed up to: {backup_key}")
        print()
    
    # Create directory
    cert_file.parent.mkdir(parents=True, exist_ok=True)
    
    print("Generating new certificate...")
    
    # Generate private key
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    print("  ✓ Private key generated")
    
    # Create certificate subject
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "California"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "Calaveras County"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Calaveras County HHS"),
        x509.NameAttribute(NameOID.COMMON_NAME, hostname),
    ])
    
    # Build Subject Alternative Names
    san_list = [
        x509.DNSName("localhost"),
        x509.DNSName("127.0.0.1"),
        x509.DNSName(hostname),
        x509.DNSName(hostname.lower()),
        x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
        x509.IPAddress(ipaddress.IPv4Address(local_ip)),
    ]
    
    # Add FQDN if different from hostname
    if fqdn != hostname and '.' in fqdn:
        san_list.append(x509.DNSName(fqdn))
        san_list.append(x509.DNSName(fqdn.lower()))
        print(f"  ✓ Added FQDN to SAN: {fqdn}")
    
    # Calculate validity dates
    valid_from = datetime.utcnow()
    valid_to = datetime.utcnow() + timedelta(days=3650)  # 10 years
    
    # Build certificate
    cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        issuer
    ).public_key(
        private_key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        valid_from
    ).not_valid_after(
        valid_to
    ).add_extension(
        x509.SubjectAlternativeName(san_list),
        critical=False,
    ).sign(private_key, hashes.SHA256())
    
    print("  ✓ Certificate signed with SHA-256")
    
    # Write certificate
    with open(cert_file, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    print(f"  ✓ Certificate saved: {cert_file}")
    
    # Write private key
    with open(key_file, "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        ))
    print(f"  ✓ Private key saved: {key_file}")
    print()
    
    # Show what's in the certificate
    print("Certificate Details:")
    print(f"  Common Name: {hostname}")
    print("  Subject Alternative Names:")
    for san in san_list:
        if isinstance(san, x509.DNSName):
            print(f"    - {san.value}")
        elif isinstance(san, x509.IPAddress):
            print(f"    - {san.value}")
    print()
    
    print("=" * 70)
    print("✅ Certificate regenerated successfully!")
    print("=" * 70)
    print()
    print("Next steps:")
    print("1. Restart the server (if running)")
    print(f"2. Access via: https://{fqdn}")
    print(f"3. Or via: https://{hostname}")
    print(f"4. Or via: https://{local_ip}")
    print()
    
    return True

if __name__ == '__main__':
    try:
        success = regenerate_certificate()
        if not success:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

