"""
================================================================================
Calaveras UniteUs ETL - Enrichment Analysis Script
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    One-time script for quick analysis of enriched November sample files.
    Analyzes demographic data, income distribution, and insurance coverage.

Features:
    - File analysis
    - Demographic data extraction
    - Statistical analysis
================================================================================
"""

def analyze_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()[1:]  # Skip header
    
    incomes = []
    households = []
    languages = []
    maritals = []
    comms = []
    medicaid = 0
    medicare = 0
    both_insurance = 0
    no_insurance = 0
    
    for line in lines:
        if not line.strip():
            continue
        parts = [p.strip('"') for p in line.split('|')]
        
        incomes.append(int(parts[20]))
        households.append(int(parts[21]))
        languages.append(parts[24])
        maritals.append(parts[18])
        comms.append(parts[30])
        
        has_medicaid = parts[27] != ""
        has_medicare = parts[29] != ""
        
        if has_medicaid and has_medicare:
            both_insurance += 1
        elif has_medicaid:
            medicaid += 1
        elif has_medicare:
            medicare += 1
        else:
            no_insurance += 1
    
    print(f"\n{'='*60}")
    print(f"Analysis of: {filepath}")
    print(f"{'='*60}")
    print(f"Total Records: {len(incomes)}")
    print(f"\nIncome Distribution:")
    print(f"  Range: ${min(incomes)} - ${max(incomes)}")
    income_brackets = {
        "No Income": sum(1 for i in incomes if i == 0),
        "Under $1,000": sum(1 for i in incomes if 0 < i < 1000),
        "$1,000-$1,999": sum(1 for i in incomes if 1000 <= i < 2000),
        "$2,000-$2,999": sum(1 for i in incomes if 2000 <= i < 3000),
        "$3,000-$4,999": sum(1 for i in incomes if 3000 <= i < 5000),
        "$5,000+": sum(1 for i in incomes if i >= 5000),
    }
    for bracket, count in income_brackets.items():
        if count > 0:
            print(f"  {bracket}: {count}")
    
    print(f"\nHousehold Sizes:")
    for size in sorted(set(households)):
        count = households.count(size)
        print(f"  Size {size}: {count}")
    
    print(f"\nInsurance Coverage:")
    print(f"  Medicaid Only: {medicaid}")
    print(f"  Medicare Only: {medicare}")
    print(f"  Both: {both_insurance}")
    print(f"  None: {no_insurance}")
    
    print(f"\nCommunication Preferences:")
    for method in sorted(set(comms)):
        count = comms.count(method)
        print(f"  {method}: {count}")
    
    print(f"\nMarital Status:")
    for status in sorted(set(maritals)):
        count = maritals.count(status)
        print(f"  {status}: {count}")
    
    print(f"\nLanguages:")
    for lang in sorted(set(languages)):
        count = languages.count(lang)
        print(f"  {lang}: {count}")

if __name__ == "__main__":
    analyze_file('temp_data_files/SAMPLE_chhsca_people_20251103.txt')
    analyze_file('temp_data_files/SAMPLE_chhsca_people_20251113.txt')
    
    print(f"\n{'='*60}")
    print("SUMMARY: Enrichment adds great variety for visualizations!")
    print(f"{'='*60}")
