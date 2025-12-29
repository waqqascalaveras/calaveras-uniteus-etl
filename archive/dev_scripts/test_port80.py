"""
Quick diagnostic script to test port 80 connectivity
"""
import socket
import sys
from pathlib import Path

def test_port(host, port):
    """Test if a port is accessible"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        print(f"Error testing {host}:{port}: {e}")
        return False

def test_http(host, port):
    """Test HTTP connection"""
    try:
        import urllib.request
        url = f"http://{host}:{port}"
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Port80-Test/1.0')
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status == 200, response.status, response.getheader('Content-Type', '')
    except Exception as e:
        return False, None, str(e)

print("=" * 60)
print("Port 80 Diagnostic Test")
print("=" * 60)

# Test localhost
print("\n1. Testing localhost:80")
localhost_socket = test_port('127.0.0.1', 80)
localhost_http = test_http('127.0.0.1', 80)
print(f"   Socket test: {'✓ PASS' if localhost_socket else '✗ FAIL'}")
if localhost_http[0]:
    print(f"   HTTP test: ✓ PASS (Status: {localhost_http[1]}, Type: {localhost_http[2]})")
else:
    print(f"   HTTP test: ✗ FAIL ({localhost_http[2]})")

# Test network IP
print("\n2. Testing network IP:80")
try:
    import socket
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    if local_ip.startswith('192.168.') or local_ip.startswith('10.') or local_ip.startswith('172.'):
        network_socket = test_port(local_ip, 80)
        network_http = test_http(local_ip, 80)
        print(f"   IP: {local_ip}")
        print(f"   Socket test: {'✓ PASS' if network_socket else '✗ FAIL'}")
        if network_http[0]:
            print(f"   HTTP test: ✓ PASS (Status: {network_http[1]}, Type: {network_http[2]})")
        else:
            print(f"   HTTP test: ✗ FAIL ({network_http[2]})")
    else:
        print(f"   IP: {local_ip} (not a private IP, skipping network test)")
except Exception as e:
    print(f"   Error getting network IP: {e}")

# Check if server is listening
print("\n3. Checking if server is listening on port 80")
try:
    import psutil
    listening = False
    for conn in psutil.net_connections(kind='inet'):
        if conn.status == 'LISTENING' and conn.laddr.port == 80:
            listening = True
            print(f"   ✓ Port 80 is LISTENING on {conn.laddr.ip}")
            break
    if not listening:
        print("   ✗ Port 80 is NOT listening")
except ImportError:
    print("   ⚠ psutil not available, skipping connection check")
except Exception as e:
    print(f"   Error checking connections: {e}")

# Check firewall
print("\n4. Firewall check")
print("   Run this command as Administrator to check firewall:")
print("   netsh advfirewall firewall show rule name=all | findstr 80")

print("\n" + "=" * 60)
print("Browser Troubleshooting:")
print("=" * 60)
print("1. Make sure you're using HTTP (not HTTPS):")
print("   ✓ http://localhost")
print("   ✓ http://192.168.86.147")
print("   ✗ https://localhost (will fail if HTTPS not enabled)")
print("\n2. Clear browser cache and try again")
print("\n3. Try a different browser (Chrome, Edge, Firefox)")
print("\n4. Check browser console (F12) for specific error messages")
print("\n5. If browser auto-redirects to HTTPS, try:")
print("   - Type 'http://' explicitly in address bar")
print("   - Clear HSTS settings (chrome://net-internals/#hsts)")
print("=" * 60)

