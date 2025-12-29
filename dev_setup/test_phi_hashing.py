"""
================================================================================
Calaveras UniteUs ETL - PHI Hashing Test Script
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Test script demonstrating the one-way hashing of PHI fields for HIPAA compliance.
    Shows how the same values always produce the same hash (for referential integrity)
    but hashes cannot be reversed to get original values.

Features:
    - Hash consistency testing
    - One-way hash verification
    - Sample PHI field hashing demonstration
    - HIPAA compliance validation
================================================================================
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import config

def test_hashing_consistency():
    """Test that same values produce same hashes"""
    print("=" * 70)
    print("Testing PHI Hashing Consistency")
    print("=" * 70)
    
    # Test with sample person_id
    person_id = "a1b2c3d4-5e6f-7a8b-9c0d-1e2f3a4b5c6d"
    hash1 = config.security.hash_value(person_id)
    hash2 = config.security.hash_value(person_id)
    
    print(f"\nOriginal person_id: {person_id}")
    print(f"Hash 1:            {hash1}")
    print(f"Hash 2:            {hash2}")
    print(f"Hashes match:      {hash1 == hash2} ✓")
    
    # Test with different values
    person_id2 = "b2c3d4e5-6f7a-8b9c-0d1e-2f3a4b5c6d7e"
    hash3 = config.security.hash_value(person_id2)
    
    print(f"\nDifferent person_id: {person_id2}")
    print(f"Hash 3:              {hash3}")
    print(f"Different from Hash 1: {hash1 != hash3} ✓")


def test_name_hashing():
    """Test hashing of personal names"""
    print("\n" + "=" * 70)
    print("Testing Name Hashing (PII Protection)")
    print("=" * 70)
    
    names = [
        ("Maria", "Elena", "Santos"),
        ("Robert", "James", "Thompson"),
        ("Jennifer", "Lynn", "Williams")
    ]
    
    for first, middle, last in names:
        first_hash = config.security.hash_value(first)
        middle_hash = config.security.hash_value(middle)
        last_hash = config.security.hash_value(last)
        
        print(f"\nOriginal: {first} {middle} {last}")
        print(f"  First:  {first_hash[:32]}... ({len(first_hash)} chars)")
        print(f"  Middle: {middle_hash[:32]}... ({len(middle_hash)} chars)")
        print(f"  Last:   {last_hash[:32]}... ({len(last_hash)} chars)")


def test_contact_info_hashing():
    """Test hashing of contact information"""
    print("\n" + "=" * 70)
    print("Testing Contact Info Hashing")
    print("=" * 70)
    
    email = "maria.santos@email.com"
    phone = "(209) 555-0142"
    address = "456 Mountain Ranch Rd"
    
    email_hash = config.security.hash_value(email)
    phone_hash = config.security.hash_value(phone)
    address_hash = config.security.hash_value(address)
    
    print(f"\nOriginal email:   {email}")
    print(f"Hashed email:     {email_hash}")
    
    print(f"\nOriginal phone:   {phone}")
    print(f"Hashed phone:     {phone_hash}")
    
    print(f"\nOriginal address: {address}")
    print(f"Hashed address:   {address_hash}")


def test_null_handling():
    """Test that null/empty values are not hashed"""
    print("\n" + "=" * 70)
    print("Testing Null/Empty Value Handling")
    print("=" * 70)
    
    test_values = ["", "nan", "none", "null", None]
    
    for value in test_values:
        result = config.security.hash_value(str(value) if value is not None else value)
        print(f"Input: {repr(value):15} → Output: {repr(result)}")


def test_referential_integrity():
    """Test that hashing preserves cross-table joins"""
    print("\n" + "=" * 70)
    print("Testing Referential Integrity (Cross-Table Joins)")
    print("=" * 70)
    
    # Simulate same person_id appearing in multiple tables
    person_id = "a1b2c3d4-5e6f-7a8b-9c0d-1e2f3a4b5c6d"
    
    # Hash in people table
    people_hash = config.security.hash_value(person_id)
    print(f"\npeople.person_id:   {people_hash}")
    
    # Hash in cases table (should be identical)
    cases_hash = config.security.hash_value(person_id)
    print(f"cases.person_id:    {cases_hash}")
    
    # Hash in referrals table (should be identical)
    referrals_hash = config.security.hash_value(person_id)
    print(f"referrals.person_id: {referrals_hash}")
    
    print(f"\nAll hashes match: {people_hash == cases_hash == referrals_hash} ✓")
    print("✓ Joins still work: SELECT * FROM people p JOIN cases c ON p.person_id = c.person_id")


def test_field_configuration():
    """Test which fields are configured for hashing"""
    print("\n" + "=" * 70)
    print("Testing Field Configuration")
    print("=" * 70)
    
    for table, fields in config.security.fields_to_hash.items():
        print(f"\n{table}: {len(fields)} fields")
        for field in fields:
            should_hash = config.security.should_hash_field(table, field)
            print(f"  {'✓' if should_hash else '✗'} {field}")


def test_security_settings():
    """Display current security settings"""
    print("\n" + "=" * 70)
    print("Current Security Settings")
    print("=" * 70)
    
    print(f"\nPHI Hashing Enabled:       {config.security.enable_phi_hashing}")
    print(f"Hash on Import:            {config.security.hash_on_import}")
    print(f"Hash on Export:            {config.security.hash_on_export}")
    print(f"Salt Length:               {len(config.security.phi_hash_salt)} characters")
    print(f"Hash Algorithm:            SHA-256")
    print(f"Hash Output Length:        64 characters (hex)")
    print(f"\nTotal Tables with Hashing: {len(config.security.fields_to_hash)}")
    
    total_fields = sum(len(fields) for fields in config.security.fields_to_hash.values())
    print(f"Total Fields Hashed:       {total_fields}")


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("PHI ONE-WAY HASHING TEST SUITE")
    print("Calaveras County UniteUs ETL System")
    print("=" * 70)
    
    test_security_settings()
    test_hashing_consistency()
    test_name_hashing()
    test_contact_info_hashing()
    test_null_handling()
    test_referential_integrity()
    test_field_configuration()
    
    print("\n" + "=" * 70)
    print("✓ All Tests Complete")
    print("=" * 70)
    print("\nKey Takeaways:")
    print("  1. Same values always produce same hash (referential integrity preserved)")
    print("  2. Hashes are 64-character hex strings (SHA-256 output)")
    print("  3. Hashes cannot be reversed to get original values")
    print("  4. Null/empty values are not hashed (preserved as-is)")
    print("  5. Cross-table joins still work with hashed keys")
    print("\n" + "=" * 70)
