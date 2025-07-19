#!/usr/bin/env python3
"""
Missing Legal Context Recovery Script

This script finds and updates act files that have legal_system_context.error
(meaning they previously had Bengali years and couldn't get legal context).
After year translation, this script re-processes those files to add proper 
legal system context.

Author: Bangladesh Legal Dataset Project
Date: July 19, 2025
"""

import json
import os
import logging
import gc
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path
from tqdm import tqdm
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('missing_legal_context_recovery.log'),
        logging.StreamHandler()
    ]
)

def load_legal_systems_data(filepath: str = 'Data/bangladesh_legal_systems.json') -> Dict:
    """Load the legal systems historical data"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load legal systems data: {e}")
        raise

def extract_year_from_act(act_data: Dict) -> Optional[int]:
    """Extract year from act data, trying multiple fields"""
    # Try different year field sources in order of preference
    year_fields = [
        'act_year',
        ('csv_metadata', 'act_year_from_csv'),
        'year'
    ]
    
    for field in year_fields:
        if isinstance(field, tuple):
            # Handle nested field like csv_metadata.act_year_from_csv
            nested_data = act_data.get(field[0], {})
            if isinstance(nested_data, dict):
                year_value = nested_data.get(field[1])
            else:
                continue
        else:
            # Handle direct field
            year_value = act_data.get(field)
        
        if year_value and year_value != 'N/A':
            # Handle different year formats
            if isinstance(year_value, int):
                if 1600 <= year_value <= 2025:  # Reasonable range
                    return year_value
            elif isinstance(year_value, str):
                # Extract 4-digit year from string
                year_match = re.search(r'\b(1[6-9]\d{2}|20[0-2]\d)\b', year_value)
                if year_match:
                    return int(year_match.group(1))
    
    # If no year found, try to extract from title
    title = act_data.get('act_title', '')
    if title:
        year_match = re.search(r'\b(1[6-9]\d{2}|20[0-2]\d)\b', title)
        if year_match:
            return int(year_match.group(1))
    
    return None

def find_legal_system_period(year: int, legal_systems: Dict) -> Optional[Dict]:
    """Find the appropriate legal system period for a given year"""
    if not year:
        return None
    
    periods = legal_systems.get('periods', [])
    
    for period in periods:
        year_from = period.get('year_from')
        year_to = period.get('year_to')
        
        if year_from and year_to:
            # Handle the case where year_from > year_to (historical data issue)
            if year_from > year_to:
                year_from, year_to = year_to, year_from
            
            if year_from <= year <= year_to:
                return period
    
    # If no exact match, find closest period
    closest_period = None
    min_distance = float('inf')
    
    for period in periods:
        year_from = period.get('year_from', 0)
        year_to = period.get('year_to', 0)
        
        if year_from > year_to:
            year_from, year_to = year_to, year_from
        
        # Calculate distance to period
        if year < year_from:
            distance = year_from - year
        elif year > year_to:
            distance = year - year_to
        else:
            distance = 0
        
        if distance < min_distance:
            min_distance = distance
            closest_period = period
    
    return closest_period

def create_legal_context(period: Optional[Dict], year: int) -> Dict:
    """Create a comprehensive legal context object from period data"""
    if not period:
        return {
            "error": "No matching legal system period found",
            "act_year": year,
            "context_added_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    
    return {
        "period_info": {
            "period_name": period.get('period_name', 'Unknown'),
            "year_range": f"{period.get('year_from', 'Unknown')}-{period.get('year_to', 'Unknown')}",
            "act_year": year
        },
        "legal_framework": {
            "primary_laws": period.get('legal_framework', {}).get('primary_laws', []),
            "court_system": period.get('legal_framework', {}).get('court_system', []),
            "legal_basis": period.get('legal_framework', {}).get('legal_basis', ''),
            "enforcement_mechanism": period.get('legal_framework', {}).get('enforcement_mechanism', '')
        },
        "government_system": {
            "type": period.get('government_system', {}).get('type', ''),
            "structure": period.get('government_system', {}).get('structure', ''),
            "revenue_collection": period.get('government_system', {}).get('revenue_collection', ''),
            "administrative_units": period.get('government_system', {}).get('administrative_units', [])
        },
        "policing_system": {
            "law_enforcement": period.get('policing_system', {}).get('law_enforcement', ''),
            "military_police": period.get('policing_system', {}).get('military_police', ''),
            "jurisdiction": period.get('policing_system', {}).get('jurisdiction', '')
        },
        "land_relations": {
            "tenure_system": period.get('land_relations', {}).get('tenure_system', ''),
            "property_rights": period.get('land_relations', {}).get('property_rights', ''),
            "revenue_system": period.get('land_relations', {}).get('revenue_system', ''),
            "peasant_status": period.get('land_relations', {}).get('peasant_status', '')
        },
        "key_characteristics": period.get('key_characteristics', []),
        "context_added_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "recovery_note": "Legal context added after year translation recovery"
    }

def has_legal_context_error(act_data: Dict) -> bool:
    """Check if the act has legal_system_context.error"""
    legal_context = act_data.get('legal_system_context', {})
    return 'error' in legal_context

def process_single_act_file(file_path: str, legal_systems: Dict, stats: Dict) -> bool:
    """Process a single act JSON file to recover missing legal context"""
    try:
        # Load the act data
        with open(file_path, 'r', encoding='utf-8') as f:
            act_data = json.load(f)
        
        # Check if this file has legal_system_context.error
        if not has_legal_context_error(act_data):
            stats['skipped_no_error'] += 1
            return True
        
        stats['files_with_error'] += 1
        
        # Extract year from act (hopefully now translated)
        year = extract_year_from_act(act_data)
        
        if not year:
            stats['still_no_year'] += 1
            logging.warning(f"Still could not extract year from {os.path.basename(file_path)}")
            # Update the error message but keep the error status
            act_data['legal_system_context'] = {
                "error": "Could not determine enactment year even after translation",
                "context_added_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "recovery_attempted": True
            }
        else:
            # Find matching legal system period
            period = find_legal_system_period(year, legal_systems)
            
            # Create legal context
            legal_context = create_legal_context(period, year)
            
            # Replace the error context with proper legal context
            act_data['legal_system_context'] = legal_context
            
            stats['years_processed'].append(year)
            
            if period and 'error' not in legal_context:
                period_name = period.get('period_name', 'Unknown')
                stats['periods_count'][period_name] = stats['periods_count'].get(period_name, 0) + 1
                stats['successfully_recovered'] += 1
                logging.info(f"✅ Recovered legal context for {os.path.basename(file_path)} (year: {year})")
            else:
                stats['still_no_period_match'] += 1
                logging.warning(f"⚠️ No period match for {os.path.basename(file_path)} (year: {year})")
        
        # Update processing metadata
        if 'processing_info' not in act_data:
            act_data['processing_info'] = {}
        
        act_data['processing_info'].update({
            'legal_context_recovery_attempted': True,
            'legal_context_recovery_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
        # Save the updated act data back to file
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(act_data, f, ensure_ascii=False, indent=2)
        
        stats['files_processed'] += 1
        return True
        
    except Exception as e:
        logging.error(f"Error processing {os.path.basename(file_path)}: {e}")
        stats['failed_files'] += 1
        return False

def recover_missing_legal_context(
    acts_directory: str = 'Data/acts',
    legal_systems_file: str = 'Data/bangladesh_legal_systems.json',
    batch_size: int = 50
):
    """
    Main function to recover missing legal system context for acts with errors
    
    Args:
        acts_directory: Directory containing individual act JSON files
        legal_systems_file: Path to legal systems data file
        batch_size: Number of files to process in each batch
    """
    
    start_time = datetime.now()
    logging.info("Starting missing legal context recovery")
    
    # Load legal systems data
    logging.info("Loading legal systems data...")
    try:
        legal_systems = load_legal_systems_data(legal_systems_file)
        logging.info(f"Successfully loaded legal systems data with {len(legal_systems.get('periods', []))} periods")
    except Exception as e:
        logging.error(f"Failed to load legal systems data: {e}")
        return
    
    # Get all JSON files in acts directory
    acts_dir = Path(acts_directory)
    if not acts_dir.exists():
        logging.error(f"Acts directory not found: {acts_directory}")
        raise FileNotFoundError(f"Acts directory not found: {acts_directory}")
    
    json_files = list(acts_dir.glob("*.json"))
    total_files = len(json_files)
    
    if total_files == 0:
        logging.warning("No JSON files found in acts directory")
        print("⚠️ No JSON files found in acts directory")
        return
    
    logging.info(f"Found {total_files} act files to check for recovery")
    
    # Initialize statistics
    stats = {
        'files_processed': 0,
        'failed_files': 0,
        'files_with_error': 0,
        'skipped_no_error': 0,
        'still_no_year': 0,
        'still_no_period_match': 0,
        'successfully_recovered': 0,
        'years_processed': [],
        'periods_count': {}
    }
    
    # Process files with progress bar
    with tqdm(total=total_files, desc="Recovering Legal Context", unit="files") as pbar:
        for i in range(0, total_files, batch_size):
            batch_files = json_files[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_files + batch_size - 1) // batch_size
            
            logging.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_files)} files)")
            
            # Process batch
            for file_path in batch_files:
                success = process_single_act_file(str(file_path), legal_systems, stats)
                pbar.update(1)
                
                # Update progress bar description
                pbar.set_postfix({
                    'WithError': stats['files_with_error'],
                    'Recovered': stats['successfully_recovered'],
                    'StillFailed': stats['still_no_year'] + stats['still_no_period_match']
                })
            
            # Memory cleanup after each batch
            gc.collect()
            
            # Log batch completion
            logging.info(f"Batch {batch_num} completed. Files with errors: {stats['files_with_error']}, Recovered: {stats['successfully_recovered']}")
    
    # Final statistics
    end_time = datetime.now()
    processing_time = end_time - start_time
    
    # Calculate year statistics
    if stats['years_processed']:
        min_year = min(stats['years_processed'])
        max_year = max(stats['years_processed'])
        avg_year = sum(stats['years_processed']) / len(stats['years_processed'])
    else:
        min_year = max_year = avg_year = 0
    
    # Log comprehensive statistics
    logging.info("=" * 60)
    logging.info("MISSING LEGAL CONTEXT RECOVERY COMPLETED")
    logging.info("=" * 60)
    logging.info(f"Total files checked: {total_files}")
    logging.info(f"Files with legal context errors: {stats['files_with_error']}")
    logging.info(f"Files skipped (no error): {stats['skipped_no_error']}")
    logging.info(f"Successfully recovered: {stats['successfully_recovered']}")
    logging.info(f"Still no year found: {stats['still_no_year']}")
    logging.info(f"Still no period match: {stats['still_no_period_match']}")
    logging.info(f"Failed files: {stats['failed_files']}")
    logging.info(f"Processing time: {processing_time}")
    
    if stats['years_processed']:
        logging.info(f"Recovered year range: {min_year} - {max_year} (avg: {avg_year:.1f})")
    
    # Period distribution for recovered files
    if stats['periods_count']:
        logging.info("\nRecovered Period Distribution:")
        for period, count in sorted(stats['periods_count'].items()):
            logging.info(f"  {period}: {count} acts")
    
    # Success rate
    if stats['files_with_error'] > 0:
        recovery_rate = (stats['successfully_recovered'] / stats['files_with_error']) * 100
        logging.info(f"\nRecovery rate: {recovery_rate:.1f}%")
    
    print(f"\n🔄 Missing legal context recovery completed!")
    print(f"📁 Checked {total_files} files")
    print(f"🔍 Found {stats['files_with_error']} files with legal context errors")
    print(f"✅ Successfully recovered {stats['successfully_recovered']} files")
    print(f"❌ Still failed: {stats['still_no_year'] + stats['still_no_period_match']} files")
    print(f"⏱️ Processing time: {processing_time}")
    
    if stats['files_with_error'] > 0:
        recovery_rate = (stats['successfully_recovered'] / stats['files_with_error']) * 100
        print(f"📊 Recovery success rate: {recovery_rate:.1f}%")

if __name__ == "__main__":
    recover_missing_legal_context()
