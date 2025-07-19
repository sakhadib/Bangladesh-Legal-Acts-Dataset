#!/usr/bin/env python3
"""
Token Count Update Script for Bangladesh Legal Acts

This script recalculates accurate token counts for all act files and updates
the token_count field in each JSON file. It counts tokens from all text content
including titles, sections, footnotes, and other text fields.

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
        logging.FileHandler('token_count_update.log'),
        logging.StreamHandler()
    ]
)

def count_tokens(text: str) -> int:
    """
    Count tokens in text using proper tokenization
    Tokens include words, numbers, punctuation marks
    """
    if not text or not isinstance(text, str):
        return 0
    
    # Remove extra whitespace and normalize
    text = text.strip()
    if not text:
        return 0
    
    # Split by whitespace and count non-empty tokens
    # This includes words, numbers, punctuation as separate tokens
    tokens = re.findall(r'\S+', text)
    return len(tokens)

def calculate_comprehensive_token_count(act_data: Dict) -> int:
    """
    Calculate comprehensive token count for an entire act including all fields
    """
    total_tokens = 0
    
    # Count tokens in main act fields
    main_fields = [
        'act_title',
        'act_no', 
        'act_year',
        'publication_date'
    ]
    
    for field in main_fields:
        if field in act_data:
            total_tokens += count_tokens(str(act_data[field]))
    
    # Count tokens in sections
    sections = act_data.get('sections', [])
    for section in sections:
        if isinstance(section, dict):
            # Count section title if present
            if 'section_title' in section:
                total_tokens += count_tokens(section['section_title'])
            
            # Count section content
            if 'section_content' in section:
                total_tokens += count_tokens(section['section_content'])
        elif isinstance(section, str):
            # Handle string sections
            total_tokens += count_tokens(section)
    
    # Count tokens in footnotes
    footnotes = act_data.get('footnotes', [])
    for footnote in footnotes:
        if isinstance(footnote, dict):
            if 'footnote_text' in footnote:
                total_tokens += count_tokens(footnote['footnote_text'])
        elif isinstance(footnote, str):
            total_tokens += count_tokens(footnote)
    
    # Count tokens in copyright info
    copyright_info = act_data.get('copyright_info', {})
    if isinstance(copyright_info, dict):
        if 'copyright_text' in copyright_info:
            total_tokens += count_tokens(copyright_info['copyright_text'])
    elif isinstance(copyright_info, str):
        total_tokens += count_tokens(copyright_info)
    
    # Count tokens in CSV metadata text fields
    csv_metadata = act_data.get('csv_metadata', {})
    if isinstance(csv_metadata, dict):
        csv_text_fields = ['act_title_from_csv', 'act_no_from_csv', 'act_year_from_csv']
        for field in csv_text_fields:
            if field in csv_metadata:
                total_tokens += count_tokens(str(csv_metadata[field]))
    
    # Count tokens in government context text fields
    govt_context = act_data.get('government_context', {})
    if isinstance(govt_context, dict):
        govt_text_fields = [
            'govt_system', 'position_head_govt', 'head_govt_name', 
            'head_govt_designation', 'how_got_power', 'period_years'
        ]
        for field in govt_text_fields:
            if field in govt_context:
                total_tokens += count_tokens(str(govt_context[field]))
    
    # Count tokens in legal system context
    legal_context = act_data.get('legal_system_context', {})
    if isinstance(legal_context, dict):
        # Count period info
        period_info = legal_context.get('period_info', {})
        if isinstance(period_info, dict):
            for key, value in period_info.items():
                if isinstance(value, str):
                    total_tokens += count_tokens(value)
        
        # Count legal framework text
        legal_framework = legal_context.get('legal_framework', {})
        if isinstance(legal_framework, dict):
            for key, value in legal_framework.items():
                if isinstance(value, str):
                    total_tokens += count_tokens(value)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, str):
                            total_tokens += count_tokens(item)
        
        # Count government system text
        govt_system = legal_context.get('government_system', {})
        if isinstance(govt_system, dict):
            for key, value in govt_system.items():
                if isinstance(value, str):
                    total_tokens += count_tokens(value)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, str):
                            total_tokens += count_tokens(item)
        
        # Count policing system text
        policing_system = legal_context.get('policing_system', {})
        if isinstance(policing_system, dict):
            for key, value in policing_system.items():
                if isinstance(value, str):
                    total_tokens += count_tokens(value)
        
        # Count land relations text
        land_relations = legal_context.get('land_relations', {})
        if isinstance(land_relations, dict):
            for key, value in land_relations.items():
                if isinstance(value, str):
                    total_tokens += count_tokens(value)
        
        # Count key characteristics
        key_characteristics = legal_context.get('key_characteristics', [])
        if isinstance(key_characteristics, list):
            for item in key_characteristics:
                if isinstance(item, str):
                    total_tokens += count_tokens(item)
        
        # Count error messages if present
        if 'error' in legal_context:
            total_tokens += count_tokens(str(legal_context['error']))
    
    return total_tokens

def process_single_act_file(file_path: str, stats: Dict) -> bool:
    """Process a single act JSON file to update token count"""
    try:
        # Load the act data
        with open(file_path, 'r', encoding='utf-8') as f:
            act_data = json.load(f)
        
        # Get current token count
        current_token_count = act_data.get('token_count', 0)
        
        # Calculate accurate token count
        accurate_token_count = calculate_comprehensive_token_count(act_data)
        
        # Update statistics
        stats['files_processed'] += 1
        stats['token_differences'].append(accurate_token_count - current_token_count)
        stats['old_total_tokens'] += current_token_count
        stats['new_total_tokens'] += accurate_token_count
        
        # Check if update is needed
        if current_token_count != accurate_token_count:
            stats['files_updated'] += 1
            stats['update_examples'].append({
                'file': os.path.basename(file_path),
                'old_count': current_token_count,
                'new_count': accurate_token_count,
                'difference': accurate_token_count - current_token_count
            })
            
            # Update the token count
            act_data['token_count'] = accurate_token_count
            
            # Update processing info
            if 'processing_info' not in act_data:
                act_data['processing_info'] = {}
            
            act_data['processing_info'].update({
                'token_count_updated': True,
                'token_count_update_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'previous_token_count': current_token_count,
                'accurate_token_count': accurate_token_count
            })
            
            # Save the updated act data
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(act_data, f, ensure_ascii=False, indent=2)
            
            logging.info(f"Updated {os.path.basename(file_path)}: {current_token_count} → {accurate_token_count}")
        else:
            stats['files_already_accurate'] += 1
        
        # Track token count distribution
        token_range = f"{(accurate_token_count // 500) * 500}-{((accurate_token_count // 500) + 1) * 500}"
        stats['token_distribution'][token_range] = stats['token_distribution'].get(token_range, 0) + 1
        
        return True
        
    except Exception as e:
        logging.error(f"Error processing {os.path.basename(file_path)}: {e}")
        stats['failed_files'] += 1
        return False

def update_token_counts(
    acts_directory: str = 'Data/acts',
    batch_size: int = 50
):
    """
    Main function to update token counts in all act files
    
    Args:
        acts_directory: Directory containing individual act JSON files
        batch_size: Number of files to process in each batch
    """
    
    start_time = datetime.now()
    logging.info("Starting token count update process")
    
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
        'files_updated': 0,
        'files_already_accurate': 0,
        'failed_files': 0,
        'old_total_tokens': 0,
        'new_total_tokens': 0,
        'token_differences': [],
        'update_examples': [],
        'token_distribution': {}
    }
    
    # Process files with progress bar
    with tqdm(total=total_files, desc="Updating Token Counts", unit="files") as pbar:
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
                    'Processed': stats['files_processed'],
                    'Updated': stats['files_updated'],
                    'Accurate': stats['files_already_accurate']
                })
            
            # Memory cleanup after each batch
            gc.collect()
            
            # Log batch completion
            logging.info(f"Batch {batch_num} completed. Updated: {stats['files_updated']}, Already accurate: {stats['files_already_accurate']}")
    
    # Final statistics
    end_time = datetime.now()
    processing_time = end_time - start_time
    
    # Calculate token statistics
    if stats['token_differences']:
        avg_difference = sum(stats['token_differences']) / len(stats['token_differences'])
        max_difference = max(stats['token_differences'])
        min_difference = min(stats['token_differences'])
    else:
        avg_difference = max_difference = min_difference = 0
    
    # Log comprehensive statistics
    logging.info("=" * 60)
    logging.info("TOKEN COUNT UPDATE COMPLETED")
    logging.info("=" * 60)
    logging.info(f"Total files processed: {stats['files_processed']}")
    logging.info(f"Files updated: {stats['files_updated']}")
    logging.info(f"Files already accurate: {stats['files_already_accurate']}")
    logging.info(f"Failed files: {stats['failed_files']}")
    logging.info(f"Processing time: {processing_time}")
    
    # Token count changes
    logging.info(f"\nToken Count Changes:")
    logging.info(f"Old total tokens: {stats['old_total_tokens']:,}")
    logging.info(f"New total tokens: {stats['new_total_tokens']:,}")
    logging.info(f"Net change: {stats['new_total_tokens'] - stats['old_total_tokens']:,}")
    
    if stats['token_differences']:
        logging.info(f"Average difference per file: {avg_difference:.1f}")
        logging.info(f"Max difference: {max_difference}")
        logging.info(f"Min difference: {min_difference}")
    
    # Show some update examples
    if stats['update_examples']:
        logging.info(f"\nUpdate Examples (first 10):")
        for i, example in enumerate(stats['update_examples'][:10]):
            logging.info(f"  {example['file']}: {example['old_count']} → {example['new_count']} (Δ{example['difference']:+d})")
    
    # Token distribution
    if stats['token_distribution']:
        logging.info(f"\nToken Count Distribution:")
        for token_range, count in sorted(stats['token_distribution'].items()):
            logging.info(f"  {token_range}: {count} acts")
    
    # Success rate
    if total_files > 0:
        success_rate = (stats['files_processed'] / total_files) * 100
        update_rate = (stats['files_updated'] / stats['files_processed']) * 100 if stats['files_processed'] > 0 else 0
        logging.info(f"\nSuccess rate: {success_rate:.1f}%")
        logging.info(f"Update rate: {update_rate:.1f}%")
    
    print(f"\n✅ Token count update completed!")
    print(f"📁 Processed {stats['files_processed']}/{total_files} files")
    print(f"🔄 Updated {stats['files_updated']} files")
    print(f"✅ {stats['files_already_accurate']} files already had accurate counts")
    print(f"📊 Total tokens: {stats['old_total_tokens']:,} → {stats['new_total_tokens']:,}")
    print(f"⏱️ Processing time: {processing_time}")
    
    if stats['files_updated'] > 0:
        update_rate = (stats['files_updated'] / stats['files_processed']) * 100
        print(f"📈 Update rate: {update_rate:.1f}%")

if __name__ == "__main__":
    update_token_counts()
