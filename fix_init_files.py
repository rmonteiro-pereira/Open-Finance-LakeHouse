#!/usr/bin/env python3
"""
Fix all __init__.py files for generated pipelines
"""

import os

# Series configuration
SERIES_CONFIG = {
    "selic_meta": "SELIC Meta",
    "over": "OVER",
    "cdi": "CDI",
    "tlp": "TLP",
    "ipca_15": "IPCA-15",
    "inpc": "INPC",
    "igp_di": "IGP-DI",
    "igp_m": "IGP-M",
    "igp_10": "IGP-10",
    "usd_brl": "USD/BRL",
    "eur_brl": "EUR/BRL",
}

def fix_init_file(series_key, series_name):
    """Fix __init__.py file for a pipeline"""
    content = f'"""\n{series_name} pipeline\n"""\n'
    
    init_path = f"src/open_finance_lakehouse/pipelines/{series_key}/__init__.py"
    with open(init_path, "w") as f:
        f.write(content)
    print(f"Fixed {init_path}")

if __name__ == "__main__":
    print("Fixing __init__.py files...")
    
    for series_key, series_name in SERIES_CONFIG.items():
        fix_init_file(series_key, series_name)
    
    print("All __init__.py files fixed!")
