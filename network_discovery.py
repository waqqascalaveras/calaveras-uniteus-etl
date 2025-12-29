#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Network Discovery Script - Analyze Intranet Configuration

Passively discovers network configuration including:
- Network interfaces and IP addresses
- DNS servers and domain configuration
- Active Directory/domain membership
- Gateway and routing information
- Open ports on local server
- Network shares and services

Author: Waqqas Hanafi
"""
import socket
import subprocess
import platform
import json
import sys
from pathlib import Path
import re

def print_section(title):
    """Print a formatted section header"""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)

def run_command(cmd, shell=True):
    """Run a command and return output"""
    try:
        result = subprocess.run(
            cmd,
            shell=shell,
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.stdout, result.stderr, result.returncode
    except Exception as e:
        return "", str(e), -1

def get_network_interfaces():
    """Get all network interfaces and their IP addresses"""
    print_section("Network Interfaces")
    
    if platform.system() == 'Windows':
        stdout, stderr, code = run_command("ipconfig /all")
        print(stdout)
        
        # Parse for key information
        interfaces = {}
        current_interface = None
        
        for line in stdout.split('\n'):
            line = line.strip()
            if line and not line.startswith(' ') and ':' in line:
                current_interface = line.split(':')[0].strip()
                interfaces[current_interface] = {}
            elif current_interface and ':' in line:
                key, value = line.split(':', 1)
                key = key.strip()
                value = value.strip()
                interfaces[current_interface][key] = value
        
        return interfaces
    else:
        stdout, stderr, code = run_command("ip addr show")
        print(stdout)
        return {}

def get_dns_servers():
    """Get DNS server configuration"""
    print_section("DNS Configuration")
    
    dns_servers = []
    
    if platform.system() == 'Windows':
        # Method 1: Use nslookup to find DNS server
        stdout, stderr, code = run_command("nslookup")
        print(stdout)
        
        # Method 2: Parse ipconfig output
        stdout, stderr, code = run_command("ipconfig /all")
        for line in stdout.split('\n'):
            if 'DNS Servers' in line or 'DNS Server' in line:
                # Extract IP from line
                match = re.search(r'\d+\.\d+\.\d+\.\d+', line)
                if match:
                    dns_servers.append(match.group())
            elif line.strip().startswith('::') or re.match(r'^\s+\d+\.\d+\.\d+\.\d+', line):
                # Additional DNS servers on subsequent lines
                match = re.search(r'\d+\.\d+\.\d+\.\d+', line)
                if match:
                    dns_servers.append(match.group())
        
        if dns_servers:
            print("\nDetected DNS Servers:")
            for dns in set(dns_servers):
                print(f"  • {dns}")
    
    return dns_servers

def get_domain_info():
    """Get Active Directory / Domain information"""
    print_section("Domain & Active Directory Information")
    
    if platform.system() == 'Windows':
        # Check domain membership
        stdout, stderr, code = run_command("systeminfo | findstr /B /C:\"Domain\"")
        print(stdout)
        
        # Get computer name and domain
        stdout, stderr, code = run_command("echo %COMPUTERNAME%")
        computer_name = stdout.strip()
        print(f"Computer Name: {computer_name}")
        
        stdout, stderr, code = run_command("echo %USERDNSDOMAIN%")
        domain = stdout.strip()
        if domain and domain != '%USERDNSDOMAIN%':
            print(f"DNS Domain: {domain}")
        
        # Get domain controller information
        print("\nDomain Controller Information:")
        stdout, stderr, code = run_command("nltest /dsgetdc:")
        if code == 0:
            print(stdout)
        else:
            print("  Not joined to a domain or unable to query domain controller")
        
        # Get AD site
        print("\nActive Directory Site:")
        stdout, stderr, code = run_command("nltest /dsgetsite")
        if code == 0:
            print(f"  Site: {stdout.strip()}")
        
        return {
            'computer_name': computer_name,
            'domain': domain,
            'domain_joined': 'WORKGROUP' not in stdout.upper()
        }
    
    return {}

def get_gateway_info():
    """Get default gateway and routing information"""
    print_section("Gateway & Routing Information")
    
    if platform.system() == 'Windows':
        # Get default gateway
        stdout, stderr, code = run_command("ipconfig | findstr /i \"Gateway\"")
        print("Default Gateway:")
        print(stdout)
        
        # Show routing table
        print("\nRouting Table:")
        stdout, stderr, code = run_command("route print")
        # Print only active routes section
        lines = stdout.split('\n')
        printing = False
        for line in lines:
            if 'Active Routes' in line:
                printing = True
            if printing:
                print(line)
                if 'Persistent Routes' in line:
                    break

def get_open_ports():
    """Check which ports are listening on this machine"""
    print_section("Open Ports on This Machine")
    
    if platform.system() == 'Windows':
        stdout, stderr, code = run_command("netstat -ano | findstr LISTENING")
        
        # Parse and display
        ports = {}
        for line in stdout.split('\n'):
            if line.strip():
                parts = line.split()
                if len(parts) >= 4:
                    addr = parts[1]
                    if ':' in addr:
                        ip, port = addr.rsplit(':', 1)
                        if port not in ports:
                            ports[port] = []
                        ports[port].append(ip)
        
        # Display sorted by port number
        print("Listening Ports:")
        for port in sorted(ports.keys(), key=lambda x: int(x) if x.isdigit() else 0):
            ips = ', '.join(set(ports[port]))
            print(f"  Port {port:>5}: {ips}")

def get_network_shares():
    """Get network shares on this machine"""
    print_section("Network Shares (This Machine)")
    
    if platform.system() == 'Windows':
        stdout, stderr, code = run_command("net share")
        print(stdout)

def get_firewall_status():
    """Check Windows Firewall status"""
    print_section("Windows Firewall Status")
    
    if platform.system() == 'Windows':
        stdout, stderr, code = run_command("netsh advfirewall show allprofiles state")
        print(stdout)

def test_dns_resolution():
    """Test DNS resolution for common names"""
    print_section("DNS Resolution Tests")
    
    test_names = [
        'localhost',
        socket.gethostname(),
        socket.getfqdn(),
    ]
    
    # Try to guess domain-based names
    fqdn = socket.getfqdn()
    if '.' in fqdn:
        domain = '.'.join(fqdn.split('.')[1:])
        test_names.extend([
            f'dc.{domain}',
            f'dns.{domain}',
        ])
    
    for name in test_names:
        try:
            ip = socket.gethostbyname(name)
            print(f"✓ {name:30} → {ip}")
        except socket.gaierror:
            print(f"✗ {name:30} → Unable to resolve")

def get_network_adapters_details():
    """Get detailed network adapter information"""
    print_section("Network Adapter Details (PowerShell)")
    
    if platform.system() == 'Windows':
        # Use PowerShell for detailed info
        cmd = "Get-NetAdapter | Select-Object Name, Status, MacAddress, LinkSpeed | Format-Table -AutoSize"
        stdout, stderr, code = run_command(f'powershell -Command "{cmd}"')
        print(stdout)
        
        # Get DNS client settings
        print("\nDNS Client Configuration:")
        cmd = "Get-DnsClientServerAddress -AddressFamily IPv4 | Format-Table -AutoSize"
        stdout, stderr, code = run_command(f'powershell -Command "{cmd}"')
        print(stdout)

def get_network_summary():
    """Create a summary of findings"""
    print_section("Network Configuration Summary")
    
    hostname = socket.gethostname()
    fqdn = socket.getfqdn()
    
    print(f"Hostname: {hostname}")
    print(f"FQDN: {fqdn}")
    
    # Try to get primary IP
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        primary_ip = s.getsockname()[0]
        s.close()
        print(f"Primary IP: {primary_ip}")
    except:
        print(f"Primary IP: Unable to determine")
    
    # Check if domain-joined
    if '.' in fqdn and fqdn != hostname:
        domain = '.'.join(fqdn.split('.')[1:])
        print(f"Domain: {domain}")
        print(f"Domain Joined: Yes")
    else:
        print(f"Domain Joined: No (Workgroup)")

def main():
    """Main function"""
    print("\n" + "█" * 80)
    print("  NETWORK DISCOVERY TOOL")
    print("  Calaveras UniteUs ETL - Network Configuration Analysis")
    print("█" * 80)
    
    try:
        # Run all discovery functions
        get_network_summary()
        get_network_interfaces()
        get_dns_servers()
        get_domain_info()
        get_gateway_info()
        get_network_adapters_details()
        get_open_ports()
        get_firewall_status()
        get_network_shares()
        test_dns_resolution()
        
        # Final recommendations
        print_section("Recommendations for DNS Setup")
        
        fqdn = socket.getfqdn()
        hostname = socket.gethostname()
        
        if '.' in fqdn and fqdn != hostname:
            domain = '.'.join(fqdn.split('.')[1:])
            print(f"\n✓ Your computer is domain-joined to: {domain}")
            print(f"\nTo set up custom DNS name 'uniteusETL':")
            print(f"\n1. Contact IT/Network Admin to add DNS record:")
            print(f"   - Name: uniteusETL")
            print(f"   - Type: A (Host)")
            print(f"   - Zone: {domain}")
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(("8.8.8.8", 80))
                primary_ip = s.getsockname()[0]
                s.close()
                print(f"   - IP Address: {primary_ip}")
            except:
                print(f"   - IP Address: [Your server's IP]")
            print(f"\n2. Users will access via:")
            print(f"   - https://uniteusETL.{domain}:443")
            print(f"   - https://uniteusETL:443 (if DNS suffix is configured)")
        else:
            print("\n⚠ Your computer is NOT domain-joined (Workgroup mode)")
            print("\nOptions for custom DNS name:")
            print("\n1. Set up local DNS server (requires DNS server software)")
            print("2. Use hosts file on each client computer:")
            print("   - Edit C:\\Windows\\System32\\drivers\\etc\\hosts")
            print(f"   - Add: [Your-IP] uniteusETL")
        
        print("\n" + "=" * 80)
        
    except KeyboardInterrupt:
        print("\n\nDiscovery interrupted by user.")
    except Exception as e:
        print(f"\n\nError during discovery: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
