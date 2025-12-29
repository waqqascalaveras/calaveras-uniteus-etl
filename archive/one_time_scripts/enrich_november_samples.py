"""
================================================================================
Calaveras UniteUs ETL - November Sample Enrichment Script
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    One-time script to enrich November 2025 sample files with diverse demographic
    data for the new dashboard visualizations. Adds household composition,
    income, insurance, and communication preference data.

Features:
    - Demographic data enrichment
    - Household composition data
    - Income and insurance data
    - Communication preferences
================================================================================
"""

import random
from datetime import datetime, timedelta

# Diverse data pools for visualizations
HOUSEHOLD_COMPOSITIONS = [
    (1, 1, 0),  # Single adult
    (2, 2, 0),  # Couple
    (2, 1, 1),  # Single parent with 1 child
    (3, 2, 1),  # Couple with 1 child
    (3, 1, 2),  # Single parent with 2 children
    (4, 2, 2),  # Couple with 2 children
    (4, 1, 3),  # Single parent with 3 children
    (5, 2, 3),  # Couple with 3 children
    (5, 3, 2),  # Extended family (3 adults, 2 children)
    (6, 2, 4),  # Large family with 4 children
    (6, 4, 2),  # Multi-generational (4 adults, 2 children)
    (7, 3, 4),  # Very large family
]

INCOME_BRACKETS = [
    0,      # No income
    450,    # Under $1,000
    750,    # Under $1,000
    1200,   # $1,000-$1,999
    1500,   # $1,000-$1,999
    1850,   # $1,000-$1,999
    2100,   # $2,000-$2,999
    2500,   # $2,000-$2,999
    2800,   # $2,000-$2,999
    3200,   # $3,000-$4,999
    3800,   # $3,000-$4,999
    4500,   # $3,000-$4,999
    5200,   # $5,000+
    6500,   # $5,000+
    8000,   # $5,000+
]

INSURANCE_TYPES = [
    ("medicaid", "CA"),
    ("medicare", None),
    ("both", "CA"),
    ("none", None),
]

COMMUNICATION_METHODS = [
    "email",
    "phone_call",
    "text_message",
    "in_person",
    "mail",
    "video_call",
]

COMMUNICATION_TIMES = [
    "morning",
    "afternoon",
    "evening",
    "any_time",
]

MARITAL_STATUSES = [
    "single",
    "married",
    "divorced",
    "widowed",
    "separated",
    "domestic_partnership",
]

LANGUAGES = [
    "English",
    "Spanish",
    "Chinese (Mandarin)",
    "Tagalog",
    "Vietnamese",
    "Korean",
    "Armenian",
    "Russian",
    "Farsi",
    "Arabic",
    "Hindi",
    "Punjabi",
    "Japanese",
    "French",
    "Portuguese",
]

def generate_medicaid_id():
    """Generate realistic California Medicaid ID"""
    return f"CA{random.randint(10000000, 99999999)}"

def generate_medicare_id():
    """Generate realistic Medicare ID (format: 1ABC-DE2-FG34)"""
    letters1 = ''.join(random.choices('ABCDEFGHJKLMNPQRSTUVWXYZ', k=3))
    letters2 = ''.join(random.choices('ABCDEFGHJKLMNPQRSTUVWXYZ', k=2))
    letters3 = ''.join(random.choices('ABCDEFGHJKLMNPQRSTUVWXYZ', k=2))
    num1 = random.randint(1, 9)
    num2 = random.randint(0, 9)
    num3 = random.randint(10, 99)
    return f"{num1}{letters1}-{letters2}{num2}-{letters3}{num3}"

def enrich_person_record(parts):
    """Add rich demographic data to a person record"""
    # parts is the split pipe-delimited record
    
    # Find column indices (header should have been read separately)
    # Assuming standard order from database_schema.py
    
    # Household composition (random selection)
    household_size, adults, children = random.choice(HOUSEHOLD_COMPOSITIONS)
    parts[21] = str(household_size)  # household_size
    parts[22] = str(adults)  # adults_in_household
    parts[23] = str(children)  # children_in_household
    
    # Income (weighted towards lower brackets)
    income = random.choices(INCOME_BRACKETS, weights=[5,8,8,10,12,10,9,8,7,6,5,4,3,2,2], k=1)[0]
    parts[20] = str(income)  # gross_monthly_income
    
    # Insurance coverage (weighted distribution)
    insurance_type, state = random.choices(
        INSURANCE_TYPES,
        weights=[35, 25, 15, 25],  # 35% medicaid, 25% medicare, 15% both, 25% none
        k=1
    )[0]
    
    if insurance_type in ("medicaid", "both"):
        parts[27] = generate_medicaid_id()  # medicaid_id
        parts[28] = "CA"  # medicaid_state
    else:
        parts[27] = ""
        parts[28] = ""
    
    if insurance_type in ("medicare", "both"):
        parts[29] = generate_medicare_id()  # medicare_id
    else:
        parts[29] = ""
    
    # Add insurance timestamps if there's insurance
    if insurance_type != "none":
        ins_date = (datetime(2025, 10, 15) + timedelta(days=random.randint(0, 20))).strftime("%Y-%m-%d %H:%M:%S")
        parts[25] = ins_date  # insurance_created_at
        parts[26] = ins_date  # insurance_updated_at
    
    # Communication preferences
    parts[30] = random.choice(COMMUNICATION_METHODS)  # preferred_communication_method
    parts[31] = random.choice(COMMUNICATION_TIMES)  # preferred_communication_time_of_day
    
    # Marital status
    parts[18] = random.choice(MARITAL_STATUSES)  # marital_status
    
    # Languages (80% English only, 15% Spanish, 5% other)
    lang_choice = random.random()
    if lang_choice < 0.80:
        parts[24] = "English"
    elif lang_choice < 0.95:
        parts[24] = "Spanish"
    else:
        parts[24] = random.choice([lang for lang in LANGUAGES if lang not in ("English", "Spanish")])
    
    return parts

def process_people_file(input_file, output_file):
    """Process and enrich people file"""
    print(f"Processing {input_file}...")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    header = lines[0].strip()
    enriched_lines = [header]
    
    for line in lines[1:]:
        if not line.strip():
            continue
        
        parts = line.strip().split('|')
        # Remove quotes
        parts = [p.strip('"') for p in parts]
        
        # Enrich with demographic data
        parts = enrich_person_record(parts)
        
        # Re-quote and join
        enriched_line = '|'.join([f'"{p}"' for p in parts])
        enriched_lines.append(enriched_line)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(enriched_lines))
    
    print(f"✅ Enriched {len(enriched_lines)-1} person records")
    print(f"   Written to {output_file}")

def main():
    """Enrich both November sample files"""
    
    # Process November 3 file
    process_people_file(
        'temp_data_files/SAMPLE_chhsca_people_20251103.txt',
        'temp_data_files/SAMPLE_chhsca_people_20251103.txt'
    )
    
    print()
    
    # Process November 13 file
    process_people_file(
        'temp_data_files/SAMPLE_chhsca_people_20251113.txt',
        'temp_data_files/SAMPLE_chhsca_people_20251113.txt'
    )
    
    print("\n" + "="*60)
    print("ENRICHMENT COMPLETE!")
    print("="*60)
    print("\nNew visualizations will show:")
    print("  • Diverse household compositions (1-7 people)")
    print("  • Income distribution across 6 brackets")
    print("  • Insurance coverage: Medicaid, Medicare, Both, None")
    print("  • Communication preferences: Email, Phone, Text, In-person, Mail, Video")
    print("  • Marital status variety")
    print("  • Multiple language preferences")

if __name__ == "__main__":
    main()
