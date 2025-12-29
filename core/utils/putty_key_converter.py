"""
PuTTY Key Converter Utility

Utility to convert PuTTY (.ppk) keys to OpenSSH format for use with SFTP.
Provides pure-Python conversion that doesn't require external tools, with
fallback to puttygen command-line tool if available.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

import logging
import subprocess
from pathlib import Path
from typing import Tuple, Optional
import base64
import struct

logger = logging.getLogger(__name__)

# Try to import cryptography for pure-Python conversion
try:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa, dsa
    from cryptography.hazmat.backends import default_backend
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    logger.warning("cryptography library not available - pure-Python PPK conversion disabled")


class PuTTYKeyParser:
    """Pure-Python PuTTY key parser - NO EXTERNAL TOOLS REQUIRED!"""
    
    @staticmethod
    def parse_ppk_file(ppk_path: Path, passphrase: Optional[str] = None) -> Tuple[bool, str, Optional[bytes]]:
        """
        Parse PuTTY PPK file and convert to OpenSSH format.
        FIXED VERSION - properly handles PPK format!
        """
        if not CRYPTO_AVAILABLE:
            return False, "cryptography library not installed", None
        
        try:
            # Read entire file
            with open(ppk_path, 'r') as f:
                content = f.read()
            
            lines = content.strip().split('\n')
            
            # Parse header - extract key type from first line
            if not lines[0].startswith('PuTTY-User-Key-File'):
                return False, "Not a valid PuTTY key file", None
            
            key_type = lines[0].split(':', 1)[1].strip() if ':' in lines[0] else ""
            logger.info(f"Detected key type: {key_type}")
            
            # Parse all key-value pairs
            data = {}
            i = 0
            while i < len(lines):
                line = lines[i].strip()
                
                if ':' in line and not line[0].isspace():
                    key, value = line.split(':', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    # Handle multi-line base64 blocks
                    if key == 'Public-Lines':
                        num_lines = int(value)
                        public_blob_lines = []
                        for j in range(1, num_lines + 1):
                            if i + j < len(lines):
                                public_blob_lines.append(lines[i + j].strip())
                        data['public_blob'] = ''.join(public_blob_lines)
                        i += num_lines
                    elif key == 'Private-Lines':
                        num_lines = int(value)
                        private_blob_lines = []
                        for j in range(1, num_lines + 1):
                            if i + j < len(lines):
                                private_blob_lines.append(lines[i + j].strip())
                        data['private_blob'] = ''.join(private_blob_lines)
                        i += num_lines
                    else:
                        data[key] = value
                
                i += 1
            
            # Check encryption
            if data.get('Encryption', 'none') != 'none':
                return False, "Encrypted keys not supported in pure-Python mode. Please use puttygen or decrypt first.", None
            
            # Get base64 blobs
            public_b64 = data.get('public_blob', '')
            private_b64 = data.get('private_blob', '')
            
            if not public_b64 or not private_b64:
                return False, f"Missing key blobs", None
            
            # Decode base64
            public_bytes = base64.b64decode(public_b64)
            private_bytes = base64.b64decode(private_b64)
            
            logger.info(f"Decoded: public={len(public_bytes)}b, private={len(private_bytes)}b")
            
            # Convert based on type
            if 'rsa' in key_type.lower():
                return PuTTYKeyParser._convert_rsa(public_bytes, private_bytes)
            elif 'dss' in key_type.lower() or 'dsa' in key_type.lower():
                return PuTTYKeyParser._convert_dsa(public_bytes, private_bytes)
            else:
                return False, f"Unsupported key type: {key_type}. Please use puttygen.", None
        
        except Exception as e:
            logger.error(f"Error parsing PPK: {e}", exc_info=True)
            return False, f"Parse error: {str(e)}", None
    
    @staticmethod
    def _read_string(data: bytes, offset: int) -> Tuple[bytes, int]:
        """Read SSH string (4-byte length + data)"""
        if len(data) < offset + 4:
            raise ValueError(f"Not enough data at offset {offset}")
        length = struct.unpack('>I', data[offset:offset+4])[0]
        if len(data) < offset + 4 + length:
            raise ValueError(f"Not enough data for string of length {length}")
        return data[offset+4:offset+4+length], offset+4+length
    
    @staticmethod
    def _read_mpint(data: bytes, offset: int) -> Tuple[int, int]:
        """Read SSH mpint"""
        string_bytes, new_offset = PuTTYKeyParser._read_string(data, offset)
        if len(string_bytes) == 0:
            return 0, new_offset
        return int.from_bytes(string_bytes, 'big'), new_offset
    
    @staticmethod
    def _convert_rsa(public_bytes: bytes, private_bytes: bytes) -> Tuple[bool, str, Optional[bytes]]:
        """Convert RSA key from PuTTY to OpenSSH format"""
        try:
            # Parse public key: [string "ssh-rsa"] [mpint e] [mpint n]
            offset = 0
            algo, offset = PuTTYKeyParser._read_string(public_bytes, offset)
            e, offset = PuTTYKeyParser._read_mpint(public_bytes, offset)
            n, offset = PuTTYKeyParser._read_mpint(public_bytes, offset)
            
            # Parse private key: [mpint d] [mpint p] [mpint q] [mpint iqmp]
            offset = 0
            d, offset = PuTTYKeyParser._read_mpint(private_bytes, offset)
            p, offset = PuTTYKeyParser._read_mpint(private_bytes, offset)
            q, offset = PuTTYKeyParser._read_mpint(private_bytes, offset)
            iqmp, offset = PuTTYKeyParser._read_mpint(private_bytes, offset)
            
            # Calculate CRT parameters
            dmp1 = d % (p - 1)
            dmq1 = d % (q - 1)
            
            # Build RSA private key
            private_key = rsa.RSAPrivateNumbers(
                p=p, q=q, d=d,
                dmp1=dmp1, dmq1=dmq1, iqmp=iqmp,
                public_numbers=rsa.RSAPublicNumbers(e=e, n=n)
            ).private_key(default_backend())
            
            # Export to OpenSSH format
            openssh_pem = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.OpenSSH,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            return True, "✅ RSA key converted successfully (pure-Python, no external tools needed!)", openssh_pem
        
        except Exception as e:
            logger.error(f"RSA conversion error: {e}", exc_info=True)
            return False, f"RSA conversion failed: {str(e)}", None
    
    @staticmethod
    def _convert_dsa(public_bytes: bytes, private_bytes: bytes) -> Tuple[bool, str, Optional[bytes]]:
        """Convert DSA key from PuTTY to OpenSSH format"""
        try:
            # Parse public: [string "ssh-dss"] [mpint p] [mpint q] [mpint g] [mpint y]
            offset = 0
            algo, offset = PuTTYKeyParser._read_string(public_bytes, offset)
            p, offset = PuTTYKeyParser._read_mpint(public_bytes, offset)
            q, offset = PuTTYKeyParser._read_mpint(public_bytes, offset)
            g, offset = PuTTYKeyParser._read_mpint(public_bytes, offset)
            y, offset = PuTTYKeyParser._read_mpint(public_bytes, offset)
            
            # Parse private: [mpint x]
            offset = 0
            x, offset = PuTTYKeyParser._read_mpint(private_bytes, offset)
            
            # Build DSA private key
            private_key = dsa.DSAPrivateNumbers(
                x=x,
                public_numbers=dsa.DSAPublicNumbers(
                    y=y,
                    parameter_numbers=dsa.DSAParameterNumbers(p=p, q=q, g=g)
                )
            ).private_key(default_backend())
            
            # Export to OpenSSH format
            openssh_pem = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.OpenSSH,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            return True, "✅ DSA key converted successfully (pure-Python, no external tools needed!)", openssh_pem
        
        except Exception as e:
            logger.error(f"DSA conversion error: {e}", exc_info=True)
            return False, f"DSA conversion failed: {str(e)}", None


class PuTTYKeyConverter:
    """Converts PuTTY format keys to OpenSSH format"""
    
    @staticmethod
    def is_putty_key(key_path: Path) -> bool:
        """Check if a file is a PuTTY format key"""
        if not key_path.exists():
            return False
        
        try:
            with open(key_path, 'r', encoding='utf-8', errors='ignore') as f:
                first_line = f.readline().strip()
                return first_line.startswith('PuTTY-User-Key-File')
        except:
            return False
    
    @staticmethod
    def is_openssh_key(key_path: Path) -> bool:
        """Check if a file is an OpenSSH format key"""
        if not key_path.exists():
            return False
        
        try:
            with open(key_path, 'r', encoding='utf-8', errors='ignore') as f:
                first_line = f.readline().strip()
                return (first_line.startswith('-----BEGIN OPENSSH PRIVATE KEY-----') or
                        first_line.startswith('-----BEGIN RSA PRIVATE KEY-----') or
                        first_line.startswith('-----BEGIN EC PRIVATE KEY-----') or
                        first_line.startswith('-----BEGIN DSA PRIVATE KEY-----') or
                        first_line.startswith('---- BEGIN SSH2 ENCRYPTED PRIVATE KEY ----'))
        except:
            return False
    
    @staticmethod
    def detect_putty_version(key_path: Path) -> Optional[int]:
        """Detect PuTTY key version (2 or 3)"""
        try:
            with open(key_path, 'r') as f:
                first_line = f.readline().strip()
                if 'PuTTY-User-Key-File-3' in first_line:
                    return 3
                elif 'PuTTY-User-Key-File-2' in first_line:
                    return 2
                elif 'PuTTY-User-Key-File-1' in first_line:
                    return 1
        except:
            pass
        return None
    
    @staticmethod
    def convert_using_puttygen(
        input_ppk: Path,
        output_key: Path,
        passphrase: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Convert PuTTY key to OpenSSH format using puttygen command.
        
        Args:
            input_ppk: Path to .ppk file
            output_key: Path for output OpenSSH key
            passphrase: Passphrase for encrypted key (optional)
        
        Returns:
            (success, message)
        """
        if not input_ppk.exists():
            return False, f"Input key not found: {input_ppk}"
        
        # Ensure output directory exists
        output_key.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            # Build puttygen command
            cmd = [
                'puttygen',
                str(input_ppk),
                '-O', 'private-openssh',
                '-o', str(output_key)
            ]
            
            if passphrase:
                # Add passphrase if provided
                cmd.extend(['-P', passphrase])
            
            # Run conversion
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                logger.info(f"Successfully converted {input_ppk} to {output_key}")
                return True, f"Conversion successful! Key saved to: {output_key}"
            else:
                error_msg = result.stderr or result.stdout or "Unknown error"
                logger.error(f"puttygen failed: {error_msg}")
                return False, f"Conversion failed: {error_msg}"
                
        except FileNotFoundError:
            msg = (
                "puttygen command not found. Please install PuTTY tools:\n"
                "  Windows: Download from https://www.chiark.greenend.org.uk/~sgtatham/putty/\n"
                "  Linux: sudo apt-get install putty-tools\n"
                "  Mac: brew install putty"
            )
            logger.warning(msg)
            return False, msg
        except subprocess.TimeoutExpired:
            return False, "Conversion timed out"
        except Exception as e:
            logger.error(f"Conversion error: {e}", exc_info=True)
            return False, f"Conversion error: {e}"
    
    @staticmethod
    def convert_key_auto(
        ppk_path: Path,
        passphrase: Optional[str] = None
    ) -> Tuple[bool, str, Optional[Path]]:
        """
        Automatically convert a PuTTY key to OpenSSH format.
        TRIES PURE-PYTHON FIRST (no tools needed), falls back to puttygen if needed.
        
        Args:
            ppk_path: Path to .ppk file
            passphrase: Passphrase for encrypted key (optional)
        
        Returns:
            (success, message, output_path)
        """
        if not ppk_path.exists():
            return False, f"Key file not found: {ppk_path}", None
        
        if not PuTTYKeyConverter.is_putty_key(ppk_path):
            return False, f"File is not a valid PuTTY key: {ppk_path}", None
        
        # Determine output path (remove .ppk extension)
        if str(ppk_path).lower().endswith('.ppk'):
            output_path = Path(str(ppk_path)[:-4])  # Remove .ppk
        else:
            output_path = ppk_path.parent / f"{ppk_path.stem}_converted"
        
        # Check if output already exists
        if output_path.exists():
            # Check if it's already a valid OpenSSH key
            if PuTTYKeyConverter.is_openssh_key(output_path):
                return True, f"✅ Key already converted: {output_path}", output_path
            else:
                # Overwrite invalid file
                logger.warning(f"Overwriting invalid converted key: {output_path}")
        
        # ============================================================
        # STEP 1: Try pure-Python conversion (NO EXTERNAL TOOLS!)
        # ============================================================
        logger.info("🔄 Attempting pure-Python PuTTY key conversion (no external tools needed)...")
        success, message, key_bytes = PuTTYKeyParser.parse_ppk_file(ppk_path, passphrase)
        
        if success and key_bytes:
            try:
                # Write the converted key
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_bytes(key_bytes)
                # Set restrictive permissions (owner read/write only)
                import os
                try:
                    os.chmod(output_path, 0o600)
                except:
                    pass  # Windows doesn't support chmod
                logger.info(f"✅ Successfully converted using pure-Python parser: {output_path}")
                return True, f"{message}\nSaved to: {output_path}", output_path
            except Exception as e:
                logger.error(f"Failed to write converted key: {e}")
                return False, f"Failed to write converted key: {str(e)}", None
        
        # ============================================================
        # STEP 2: Fallback to puttygen if pure-Python failed
        # ============================================================
        logger.info(f"Pure-Python conversion not available ({message}). Trying puttygen as fallback...")
        success, message = PuTTYKeyConverter.convert_using_puttygen(
            ppk_path, output_path, passphrase
        )
        
        if success:
            return True, message, output_path
        else:
            # Both methods failed
            return False, f"Conversion failed. Pure-Python: {message}\nPlease install PuTTY tools or convert manually.", None
    
    @staticmethod
    def get_conversion_instructions(ppk_path: Path) -> str:
        """Get manual conversion instructions for a PuTTY key"""
        version = PuTTYKeyConverter.detect_putty_version(ppk_path)
        
        return f"""
╔══════════════════════════════════════════════════════════════╗
║         PuTTY Key Conversion Required                        ║
╚══════════════════════════════════════════════════════════════╝

Your key file is in PuTTY format (version {version or 'unknown'}), which needs
to be converted to OpenSSH format for compatibility.

📋 CONVERSION OPTIONS:

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Option 1: Using PuTTYgen (GUI - Easiest)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. Open PuTTYgen (comes with PuTTY installation)
2. Click "Load" and select your file:
   {ppk_path}
3. Enter passphrase if prompted
4. Go to menu: Conversions → Export OpenSSH key
5. Save as: keys\\calco-uniteus-sftp (no extension)
6. Update SFTP settings to use the new key path

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Option 2: Using Command Line
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Windows (PowerShell):
    puttygen {ppk_path} -O private-openssh -o keys\\calco-uniteus-sftp

Linux/Mac:
    puttygen {ppk_path} -O private-openssh -o keys/calco-uniteus-sftp

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Option 3: Using ssh-keygen (if available)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Note: This only works for PuTTY v3 format keys
    ssh-keygen -i -f {ppk_path} > keys/calco-uniteus-sftp

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📥 Download PuTTY Tools:
   https://www.chiark.greenend.org.uk/~sgtatham/putty/latest.html

After conversion:
✓ Update the Private Key Path in SFTP settings
✓ Keep both keys backed up securely
✓ Test the connection

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""


def convert_putty_key_interactive(ppk_path: str, output_path: str = None) -> bool:
    """
    Interactive PuTTY key conversion with user prompts.
    
    Args:
        ppk_path: Path to .ppk file
        output_path: Optional output path (defaults to same name without .ppk)
    
    Returns:
        True if successful
    """
    ppk = Path(ppk_path)
    
    if not ppk.exists():
        print(f"❌ Error: File not found: {ppk}")
        return False
    
    if not PuTTYKeyConverter.is_putty_key(ppk):
        print(f"❌ Error: Not a valid PuTTY key file: {ppk}")
        return False
    
    # Determine output path
    if output_path is None:
        output_path = str(ppk).replace('.ppk', '')
    output = Path(output_path)
    
    # Check if output exists
    if output.exists():
        response = input(f"⚠️  Output file already exists: {output}\nOverwrite? (y/N): ")
        if response.lower() != 'y':
            print("❌ Conversion cancelled")
            return False
    
    # Get passphrase
    passphrase = input("🔑 Enter passphrase (or press Enter if none): ").strip()
    if not passphrase:
        passphrase = None
    
    print(f"\n🔄 Converting {ppk} → {output}...")
    
    # Attempt conversion
    success, message = PuTTYKeyConverter.convert_using_puttygen(
        ppk, output, passphrase
    )
    
    if success:
        print(f"✅ {message}")
        print(f"\n📝 Next steps:")
        print(f"   1. Update SFTP settings to use: {output}")
        print(f"   2. Test the connection")
        print(f"   3. Delete the .ppk file (optional)")
        return True
    else:
        print(f"❌ {message}")
        print("\n" + PuTTYKeyConverter.get_conversion_instructions(ppk))
        return False


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python putty_key_converter.py <path_to_ppk_file> [output_path]")
        print("\nExample:")
        print("  python putty_key_converter.py keys/mykey.ppk")
        print("  python putty_key_converter.py keys/mykey.ppk keys/mykey_openssh")
        sys.exit(1)
    
    ppk_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    success = convert_putty_key_interactive(ppk_file, output_file)
    sys.exit(0 if success else 1)

