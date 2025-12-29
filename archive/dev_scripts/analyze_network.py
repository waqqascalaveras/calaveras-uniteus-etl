#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
Calaveras UniteUs ETL - Network Analysis Script
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Analyzes network configuration and provides specific deployment
    recommendations for the UniteUs ETL server on the company LAN.

Features:
    - Detects hostname and FQDN
    - Tests DNS/WINS resolution
    - Checks port availability (80, 443, 8000)
    - Analyzes network interfaces
    - Provides deployment recommendations
================================================================================
"""

import socket
import subprocess
import sys
import platform
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import json

def run_command(cmd: List[str], capture_output: bool = True) -> Tuple[int, str, str]:
    """Run a command and return exit code, stdout, stderr"""
    try:
        result = subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True,
            timeout=10
        )
        stdout = result.stdout if capture_output else ""
        stderr = result.stderr if capture_output else ""
        return result.returncode, stdout, stderr
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out"
    except Exception as e:
        return -1, "", str(e)

def get_hostname_info() -> Dict:
    """Get hostname and FQDN information"""
    info = {
        'hostname': socket.gethostname(),
        'fqdn': None,
        'domain': None,
        'computer_name': None,
        'dns_suffix': None
    }
    
    try:
        # Get FQDN
        fqdn = socket.getfqdn()
        info['fqdn'] = fqdn if fqdn != info['hostname'] else None
        
        # On Windows, get more detailed info
        if platform.system() == 'Windows':
            # Get computer name and domain
            exit_code, stdout, _ = run_command(['net', 'config', 'workstation'])
            if exit_code == 0:
                for line in stdout.split('\n'):
                    if 'Computer name' in line:
                        info['computer_name'] = line.split('Computer name')[1].strip()
                    elif 'Full Computer name' in line:
                        full_name = line.split('Full Computer name')[1].strip()
                        if '.' in full_name:
                            parts = full_name.split('.', 1)
                            info['computer_name'] = parts[0]
                            info['domain'] = parts[1]
                            info['fqdn'] = full_name
                    elif 'Workstation domain' in line:
                        info['domain'] = line.split('Workstation domain')[1].strip()
            
            # Get DNS suffix
            exit_code, stdout, _ = run_command(['ipconfig', '/all'])
            if exit_code == 0:
                for line in stdout.split('\n'):
                    if 'Primary Dns Suffix' in line:
                        info['dns_suffix'] = line.split(':')[1].strip() if ':' in line else None
                        break
    except Exception as e:
        info['error'] = str(e)
    
    return info

def get_network_interfaces() -> List[Dict]:
    """Get network interface information"""
    interfaces = []
    
    try:
        if platform.system() == 'Windows':
            exit_code, stdout, _ = run_command(['ipconfig', '/all'])
            if exit_code == 0:
                current_interface = None
                for line in stdout.split('\n'):
                    line = line.strip()
                    if 'adapter' in line.lower() or 'Ethernet adapter' in line or 'Wireless LAN adapter' in line:
                        if current_interface:
                            interfaces.append(current_interface)
                        current_interface = {
                            'name': line.replace('adapter', '').replace('Ethernet', '').replace('Wireless LAN', '').strip(': '),
                            'type': 'Ethernet' if 'Ethernet' in line else 'Wireless' if 'Wireless' in line else 'Unknown'
                        }
                    elif current_interface:
                        if 'IPv4 Address' in line or 'IPv4' in line:
                            ip = line.split(':')[1].strip() if ':' in line else None
                            if ip and ip != '127.0.0.1':
                                current_interface['ipv4'] = ip.split('(')[0].strip()
                        elif 'Subnet Mask' in line:
                            current_interface['subnet'] = line.split(':')[1].strip() if ':' in line else None
                        elif 'Default Gateway' in line:
                            current_interface['gateway'] = line.split(':')[1].strip() if ':' in line else None
                        elif 'DNS Servers' in line or 'DNS' in line:
                            dns = line.split(':')[1].strip() if ':' in line else None
                            if dns:
                                current_interface['dns'] = dns.split()[0] if dns.split() else None
                
                if current_interface:
                    interfaces.append(current_interface)
        else:
            # Linux/Mac
            exit_code, stdout, _ = run_command(['hostname', '-I'])
            if exit_code == 0:
                ips = stdout.strip().split()
                for ip in ips:
                    if not ip.startswith('127.'):
                        interfaces.append({
                            'name': 'Primary',
                            'type': 'Unknown',
                            'ipv4': ip
                        })
    except Exception as e:
        interfaces.append({'error': str(e)})
    
    return interfaces

def test_dns_resolution(hostname: str) -> Dict:
    """Test DNS resolution for a hostname"""
    results = {
        'hostname': hostname,
        'resolves': False,
        'ip_addresses': [],
        'reverse_dns': None,
        'error': None
    }
    
    try:
        # Forward lookup
        ip = socket.gethostbyname(hostname)
        results['resolves'] = True
        results['ip_addresses'].append(ip)
        
        # Reverse lookup
        try:
            reverse = socket.gethostbyaddr(ip)
            results['reverse_dns'] = reverse[0]
        except:
            pass
        
        # Try to get all IPs
        try:
            all_ips = socket.gethostbyname_ex(hostname)
            results['ip_addresses'] = list(all_ips[2])
        except:
            pass
    except socket.gaierror as e:
        results['error'] = f"DNS resolution failed: {e}"
    except Exception as e:
        results['error'] = str(e)
    
    return results

def test_wins_resolution(hostname: str) -> Dict:
    """Test WINS/NetBIOS resolution (Windows)"""
    results = {
        'hostname': hostname,
        'wins_resolves': False,
        'error': None
    }
    
    if platform.system() != 'Windows':
        results['error'] = 'WINS only available on Windows'
        return results
    
    try:
        exit_code, stdout, stderr = run_command(['nbtstat', '-A', hostname])
        if exit_code == 0 and 'Name' in stdout:
            results['wins_resolves'] = True
        else:
            results['error'] = 'WINS resolution failed or service not available'
    except Exception as e:
        results['error'] = str(e)
    
    return results

def check_port_available(port: int, host: str = '0.0.0.0') -> Dict:
    """Check if a port is available"""
    result = {
        'port': port,
        'available': False,
        'in_use_by': None,
        'error': None
    }
    
    try:
        # Try to bind to the port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
            result['available'] = True
            sock.close()
        except OSError as e:
            result['available'] = False
            result['error'] = str(e)
            
            # Try to find what's using it (Windows)
            if platform.system() == 'Windows':
                exit_code, stdout, _ = run_command(['netstat', '-ano'])
                if exit_code == 0:
                    for line in stdout.split('\n'):
                        if f':{port}' in line and 'LISTENING' in line:
                            parts = line.split()
                            if len(parts) >= 5:
                                pid = parts[-1]
                                # Get process name
                                exit_code2, stdout2, _ = run_command(['tasklist', '/FI', f'PID eq {pid}', '/FO', 'CSV'])
                                if exit_code2 == 0 and pid in stdout2:
                                    for proc_line in stdout2.split('\n'):
                                        if pid in proc_line:
                                            proc_name = proc_line.split(',')[0].strip('"')
                                            result['in_use_by'] = f"{proc_name} (PID: {pid})"
                                            break
    except Exception as e:
        result['error'] = str(e)
    
    return result

def check_admin_privileges() -> Dict:
    """Check if running with administrator privileges"""
    result = {
        'is_admin': False,
        'can_use_privileged_ports': False,
        'method': None
    }
    
    if platform.system() == 'Windows':
        try:
            import ctypes
            is_admin = ctypes.windll.shell32.IsUserAnAdmin() != 0
            result['is_admin'] = is_admin
            result['method'] = 'Windows UAC check'
            result['can_use_privileged_ports'] = is_admin
        except:
            result['method'] = 'Could not check (ctypes unavailable)'
    else:
        # Unix-like: check if EUID is 0
        try:
            result['is_admin'] = os.geteuid() == 0
            result['method'] = 'Unix EUID check'
            result['can_use_privileged_ports'] = result['is_admin']
        except:
            result['method'] = 'Could not check'
    
    return result

def get_recommendations(analysis: Dict) -> List[str]:
    """Generate deployment recommendations based on analysis"""
    recommendations = []
    
    # Port recommendations
    port_80 = analysis.get('ports', {}).get(80, {})
    port_443 = analysis.get('ports', {}).get(443, {})
    port_8000 = analysis.get('ports', {}).get(8000, {})
    is_admin = analysis.get('admin', {}).get('is_admin', False)
    
    if port_80.get('available', False) and is_admin:
        recommendations.append("✅ Port 80 is available - Users can access via http://HOSTNAME (no port number needed)")
        recommendations.append("   Recommended: Use port 80 for HTTP deployment")
    elif not port_80.get('available', False) and is_admin:
        recommendations.append(f"⚠️  Port 80 is in use by: {port_80.get('in_use_by', 'Unknown')}")
        recommendations.append("   Option 1: Stop the service using port 80 (e.g., IIS)")
        recommendations.append("   Option 2: Use port 8000 (users will need :8000 in URL)")
        recommendations.append("   Option 3: Use port 443 with HTTPS (recommended for security)")
    
    if port_443.get('available', False) and is_admin:
        recommendations.append("✅ Port 443 is available - Users can access via https://HOSTNAME (no port number needed)")
        recommendations.append("   Recommended: Use port 443 with HTTPS + County CA certificate for production")
    
    if not is_admin:
        recommendations.append("⚠️  Not running as Administrator - Cannot use ports 80 or 443")
        recommendations.append("   Option 1: Run launch.pyw as Administrator (right-click → Run as administrator)")
        recommendations.append("   Option 2: Use port 8000 (no admin needed, but users must include :8000)")
        recommendations.append("   Option 3: Reserve port 80 using: netsh http add urlacl url=http://+:80/ user=Everyone")
    
    # DNS recommendations
    hostname_info = analysis.get('hostname_info', {})
    dns_test = analysis.get('dns_tests', {}).get('hostname', {})
    wins_test = analysis.get('wins_tests', {}).get('hostname', {})
    
    if dns_test.get('resolves', False):
        recommendations.append(f"✅ DNS resolution works - Users can access via http://{hostname_info.get('hostname', 'HOSTNAME')}")
    else:
        recommendations.append(f"⚠️  DNS resolution may not work - Users may need to use IP address: {analysis.get('primary_ip', 'IP_ADDRESS')}")
    
    if wins_test.get('wins_resolves', False):
        recommendations.append("✅ WINS/NetBIOS resolution works - Hostname will work on Windows network")
    else:
        recommendations.append("⚠️  WINS resolution not available - DNS or IP address required")
    
    # FQDN recommendations
    fqdn = hostname_info.get('fqdn') or hostname_info.get('domain')
    if fqdn:
        recommendations.append(f"✅ FQDN available: {fqdn}")
        recommendations.append(f"   Users can access via: http://{fqdn}")
    
    # Security recommendations
    recommendations.append("")
    recommendations.append("🔒 Security Recommendations:")
    recommendations.append("   1. Use HTTPS (port 443) with County CA certificate for production")
    recommendations.append("   2. Configure Windows Firewall to allow the selected port")
    recommendations.append("   3. Ensure Active Directory authentication is properly configured")
    recommendations.append("   4. Use IP restrictions if needed (already configured in auth.py)")
    
    return recommendations

def main():
    """Main analysis function"""
    print("=" * 70)
    print("Calaveras UniteUs ETL - Network Analysis")
    print("=" * 70)
    print()
    
    analysis = {}
    
    # 1. Hostname Information
    print("📋 Gathering hostname information...")
    hostname_info = get_hostname_info()
    analysis['hostname_info'] = hostname_info
    print(f"   Hostname: {hostname_info.get('hostname', 'Unknown')}")
    if hostname_info.get('fqdn'):
        print(f"   FQDN: {hostname_info.get('fqdn')}")
    if hostname_info.get('domain'):
        print(f"   Domain: {hostname_info.get('domain')}")
    if hostname_info.get('dns_suffix'):
        print(f"   DNS Suffix: {hostname_info.get('dns_suffix')}")
    print()
    
    # 2. Network Interfaces
    print("🌐 Analyzing network interfaces...")
    interfaces = get_network_interfaces()
    analysis['interfaces'] = interfaces
    primary_ip = None
    for iface in interfaces:
        if iface.get('ipv4') and not iface['ipv4'].startswith('127.'):
            primary_ip = iface['ipv4']
            print(f"   Interface: {iface.get('name', 'Unknown')} ({iface.get('type', 'Unknown')})")
            print(f"   IPv4: {iface.get('ipv4')}")
            if iface.get('subnet'):
                print(f"   Subnet: {iface.get('subnet')}")
            if iface.get('gateway'):
                print(f"   Gateway: {iface.get('gateway')}")
            break
    analysis['primary_ip'] = primary_ip
    print()
    
    # 3. DNS Resolution Tests
    print("🔍 Testing DNS resolution...")
    hostname = hostname_info.get('hostname', 'localhost')
    dns_test = test_dns_resolution(hostname)
    analysis['dns_tests'] = {'hostname': dns_test}
    if dns_test.get('resolves'):
        print(f"   ✅ {hostname} resolves to: {', '.join(dns_test.get('ip_addresses', []))}")
        if dns_test.get('reverse_dns'):
            print(f"   Reverse DNS: {dns_test.get('reverse_dns')}")
    else:
        print(f"   ❌ {hostname} does not resolve: {dns_test.get('error', 'Unknown error')}")
    
    # Test FQDN if available
    if hostname_info.get('fqdn'):
        fqdn_test = test_dns_resolution(hostname_info['fqdn'])
        analysis['dns_tests']['fqdn'] = fqdn_test
        if fqdn_test.get('resolves'):
            print(f"   ✅ {hostname_info['fqdn']} resolves to: {', '.join(fqdn_test.get('ip_addresses', []))}")
        else:
            print(f"   ❌ {hostname_info['fqdn']} does not resolve: {fqdn_test.get('error', 'Unknown error')}")
    print()
    
    # 4. WINS Resolution (Windows only)
    if platform.system() == 'Windows':
        print("🔍 Testing WINS/NetBIOS resolution...")
        wins_test = test_wins_resolution(hostname)
        analysis['wins_tests'] = {'hostname': wins_test}
        if wins_test.get('wins_resolves'):
            print(f"   ✅ WINS resolution works for {hostname}")
        else:
            print(f"   ⚠️  WINS resolution: {wins_test.get('error', 'Not available')}")
        print()
    
    # 5. Port Availability
    print("🔌 Checking port availability...")
    ports_to_check = [80, 443, 8000]
    port_results = {}
    for port in ports_to_check:
        result = check_port_available(port)
        port_results[port] = result
        status = "✅ Available" if result.get('available') else f"❌ In use by: {result.get('in_use_by', 'Unknown')}"
        print(f"   Port {port}: {status}")
    analysis['ports'] = port_results
    print()
    
    # 6. Administrator Privileges
    print("👤 Checking administrator privileges...")
    admin_info = check_admin_privileges()
    analysis['admin'] = admin_info
    if admin_info.get('is_admin'):
        print("   ✅ Running with administrator privileges")
        print("   ✅ Can use ports 80 and 443")
    else:
        print("   ⚠️  Not running as administrator")
        print("   ⚠️  Cannot use ports 80 or 443 (requires admin)")
    print()
    
    # 7. Recommendations
    print("=" * 70)
    print("📊 DEPLOYMENT RECOMMENDATIONS")
    print("=" * 70)
    print()
    recommendations = get_recommendations(analysis)
    for rec in recommendations:
        print(rec)
    print()
    
    # 8. Save results
    output_file = Path("network_analysis.json")
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, indent=2, default=str)
        print(f"💾 Analysis saved to: {output_file}")
    except Exception as e:
        print(f"⚠️  Could not save analysis: {e}")
    
    print()
    print("=" * 70)
    print("Analysis complete!")
    print("=" * 70)

if __name__ == '__main__':
    try:
        import os
        main()
    except KeyboardInterrupt:
        print("\n\nAnalysis interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

