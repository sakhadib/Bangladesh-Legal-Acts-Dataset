#!/usr/bin/env python3
"""
Year Translation Script for Bangladesh Legal Acts

This script translates Bengali years to English and standardizes year formats
across all act files. It handles:
1. Bengali numerals to English numerals
2. Falls back to CSV metadata if year is not in standard format
3. Processes all individual JSON files in acts/ folder

Author: Bangladesh Legal Dataset Project
Date: July 19, 2025
"""

import json
import os
import logging
import gc
import re
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('year_translation.log'),
        logging.StreamHandler()
    ]
)

# Bengali to English digit mapping
BENGALI_TO_ENGLISH = {
    '০': '0', '১': '1', '২': '2', '৩': '3', '৪': '4',
    '৫': '5', '৬': '6', '৭': '7', '৮': '8', '৯': '9'
}

def translate_bengali_to_english(text: str) -> str:
    """Translate Bengali numerals to English numerals"""
    if not text:
        return text
    
    # Convert Bengali digits to English
    result = str(text)
    for bengali, english in BENGALI_TO_ENGLISH.items():
        result = result.replace(bengali, english)
    
    return result

def extract_and_standardize_year(year_value: Any) -> Optional[str]:
    """Extract and standardize year from various formats"""
    if not year_value or year_value == 'N/A':
        return None
    
    # Convert to string for processing
    year_str = str(year_value).strip()
    
    # Translate Bengali numerals if present
    year_str = translate_bengali_to_english(year_str)
    
    # Try to extract 4-digit year (1600-2025)
    year_match = re.search(r'\b(1[6-9]\d{2}|20[0-2]\d)\b', year_str)
    if year_match:
        return year_match.group(1)
    
    # If direct extraction fails, try to find any 4-digit number
    year_match = re.search(r'\d{4}', year_str)
    if year_match:
        year_num = int(year_match.group(0))
        if 1600 <= year_num <= 2025:
            return str(year_num)
    
    return None

def process_single_act_file(file_path: str, stats: Dict) -> bool:
    """Process a single act JSON file to standardize year format"""
    try:
        # Load the act data
        with open(file_path, 'r', encoding='utf-8') as f:
            act_data = json.load(f)
        
        original_year = act_data.get('act_year', '')
        stats['files_processed'] += 1
        
        # First try to standardize the current act_year
        standardized_year = extract_and_standardize_year(original_year)
        
        if standardized_year:
            # Successfully extracted/translated year
            if original_year != standardized_year:
                act_data['act_year'] = standardized_year
                stats['years_translated'] += 1
                stats['translation_examples'].append({
                    'file': os.path.basename(file_path),
                    'original': original_year,
                    'translated': standardized_year
                })
                logging.info(f"Translated year in {os.path.basename(file_path)}: '{original_year}' → '{standardized_year}'")
            else:
                stats['years_already_standard'] += 1
        else:
            # Could not standardize current year, try CSV metadata
            csv_metadata = act_data.get('csv_metadata', {})
            csv_year = csv_metadata.get('act_year_from_csv')
            
            if csv_year:
                standardized_csv_year = extract_and_standardize_year(csv_year)
                if standardized_csv_year:
                    act_data['act_year'] = standardized_csv_year
                    stats['years_from_csv'] += 1
                    stats['csv_fallback_examples'].append({
                        'file': os.path.basename(file_path),
                        'original': original_year,
                        'csv_original': csv_year,
                        'final': standardized_csv_year
                    })
                    logging.info(f"Used CSV year for {os.path.basename(file_path)}: '{csv_year}' → '{standardized_csv_year}'")
                else:
                    # CSV year also couldn't be standardized
                    stats['years_failed'] += 1
                    stats['failed_examples'].append({
                        'file': os.path.basename(file_path),
                        'act_year': original_year,
                        'csv_year': csv_year
                    })
                    logging.warning(f"Could not standardize year for {os.path.basename(file_path)}: act_year='{original_year}', csv_year='{csv_year}'")
            else:
                # No CSV metadata available
                stats['years_failed'] += 1
                stats['failed_examples'].append({
                    'file': os.path.basename(file_path),
                    'act_year': original_year,
                    'csv_year': None
                })
                logging.warning(f"Could not standardize year for {os.path.basename(file_path)}: act_year='{original_year}', no CSV data")
        
        # Add processing metadata
        if 'processing_info' not in act_data:
            act_data['processing_info'] = {}
        
        act_data['processing_info'].update({
            'year_standardized': True,
            'year_standardization_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
        # Save the updated act data back to file
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(act_data, f, ensure_ascii=False, indent=2)
        
        return True
        
    except Exception as e:
        logging.error(f"Error processing {os.path.basename(file_path)}: {e}")
        stats['failed_files'] += 1
        return False

def standardize_years_in_acts(
    acts_directory: str = 'Data/acts',
    batch_size: int = 50
):
    """
    Main function to standardize years in all act files
    
    Args:
        acts_directory: Directory containing individual act JSON files
        batch_size: Number of files to process in each batch
    """
    
    start_time = datetime.now()
    logging.info("Starting year standardization process")
    
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
    
    logging.info(f"Found {total_files} act files to process")
    
    # Initialize statistics
    stats = {
        'files_processed': 0,
        'failed_files': 0,
        'years_translated': 0,
        'years_already_standard': 0,
        'years_from_csv': 0,
        'years_failed': 0,
        'translation_examples': [],
        'csv_fallback_examples': [],
        'failed_examples': []
    }
    
    # Process files with progress bar
    with tqdm(total=total_files, desc="Standardizing Years", unit="files") as pbar:
        for i in range(0, total_files, batch_size):
            batch_files = json_files[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_files + batch_size - 1) // batch_size
            
            logging.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_files)} files)")
            
            # Process batch
            for file_path in batch_files:
                success = process_single_act_file(str(file_path), stats)
                pbar.update(1)
                
                # Update progress bar description
                pbar.set_postfix({
                    'Translated': stats['years_translated'],
                    'From CSV': stats['years_from_csv'],
                    'Failed': stats['years_failed']
                })
            
            # Memory cleanup after each batch
            gc.collect()
            
            # Log batch completion
            logging.info(f"Batch {batch_num} completed. Processed: {stats['files_processed']}, Failed: {stats['failed_files']}")
    
    # Final statistics
    end_time = datetime.now()
    processing_time = end_time - start_time
    
    # Log comprehensive statistics
    logging.info("=" * 60)
    logging.info("YEAR STANDARDIZATION COMPLETED")
    logging.info("=" * 60)
    logging.info(f"Total files found: {total_files}")
    logging.info(f"Successfully processed: {stats['files_processed']}")
    logging.info(f"Failed files: {stats['failed_files']}")
    logging.info(f"Years translated: {stats['years_translated']}")
    logging.info(f"Years already standard: {stats['years_already_standard']}")
    logging.info(f"Years from CSV: {stats['years_from_csv']}")
    logging.info(f"Years failed: {stats['years_failed']}")
    logging.info(f"Processing time: {processing_time}")
    
    # Show translation examples
    if stats['translation_examples']:
        logging.info("\nTranslation Examples:")
        for example in stats['translation_examples'][:5]:  # Show first 5
            logging.info(f"  {example['file']}: '{example['original']}' → '{example['translated']}'")
        if len(stats['translation_examples']) > 5:
            logging.info(f"  ... and {len(stats['translation_examples']) - 5} more")
    
    # Show CSV fallback examples
    if stats['csv_fallback_examples']:
        logging.info("\nCSV Fallback Examples:")
        for example in stats['csv_fallback_examples'][:5]:  # Show first 5
            logging.info(f"  {example['file']}: '{example['csv_original']}' → '{example['final']}'")
        if len(stats['csv_fallback_examples']) > 5:
            logging.info(f"  ... and {len(stats['csv_fallback_examples']) - 5} more")
    
    # Show failed examples
    if stats['failed_examples']:
        logging.info("\nFailed Examples:")
        for example in stats['failed_examples'][:5]:  # Show first 5
            logging.info(f"  {example['file']}: act_year='{example['act_year']}', csv_year='{example.get('csv_year', 'None')}'")
        if len(stats['failed_examples']) > 5:
            logging.info(f"  ... and {len(stats['failed_examples']) - 5} more")
    
    # Success rate
    if total_files > 0:
        success_rate = (stats['files_processed'] / total_files) * 100
        standardization_rate = ((stats['years_translated'] + stats['years_already_standard'] + stats['years_from_csv']) / stats['files_processed']) * 100 if stats['files_processed'] > 0 else 0
        logging.info(f"\nSuccess rate: {success_rate:.1f}%")
        logging.info(f"Year standardization rate: {standardization_rate:.1f}%")
    
    print(f"\n✅ Year standardization completed!")
    print(f"📁 Processed {stats['files_processed']}/{total_files} files")
    print(f"🔤 Translated {stats['years_translated']} Bengali years")
    print(f"📋 Used CSV data for {stats['years_from_csv']} files")
    print(f"✅ {stats['years_already_standard']} years already standard")
    print(f"❌ Failed to standardize {stats['years_failed']} years")
    print(f"⏱️ Processing time: {processing_time}")

def test_translation():
    """Test function to verify translation logic"""
    test_cases = [
        "১৮৬০",  # Bengali 1860
        "১৯৭১",  # Bengali 1971
        "2024",   # English 2024
        "Act XXI of ১৮৬০",  # Mixed
        "১৯৪৭ সালের আইন",  # Bengali with text
        "N/A",
        "",
        None
    ]
    
    print("Testing year translation:")
    for test in test_cases:
        result = extract_and_standardize_year(test)
        print(f"'{test}' → '{result}'")

if __name__ == "__main__":
    # Uncomment to run tests
    # test_translation()
    
    # Run the main standardization process
    standardize_years_in_acts()
