#!/usr/bin/env python3
"""
Individual Act Files Reducer and Government Context Fusion Script

This script processes each individual JSON file in the acts/ folder to:
1. Fill missing data from CSV metadata (like reducer.py)
2. Add token counting and language detection
3. Add historical government context based on enactment year

Optimized for processing 1500+ individual files efficiently.

Author: Bangladesh Legal Dataset Project
Date: July 19, 2025
"""

import json
import os
import sys
import gc
import re
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from collections import Counter

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reduceAndFuseGovt.log'),
        logging.StreamHandler()
    ]
)

def load_govt_data(govt_file_path: str) -> Dict[int, Dict]:
    """Load government context data and create lookup table"""
    try:
        with open(govt_file_path, 'r', encoding='utf-8') as f:
            govt_data = json.load(f)
        
        logging.info(f"✓ Loaded {len(govt_data)} government records")
        
        # Create optimized lookup table
        logging.info("🔍 Creating government lookup table...")
        govt_lookup = {}
        
        for govt_period in govt_data:
            year_from = govt_period.get('year_from', 0)
            year_to = govt_period.get('year_to', 9999)
            
            govt_context = {
                'govt_system': govt_period.get('govt_system', 'Unknown'),
                'position_head_govt': govt_period.get('position_head_govt', 'Unknown'),
                'head_govt_name': govt_period.get('head_govt_name', 'Unknown'),
                'head_govt_designation': govt_period.get('head_govt_designation', 'Unknown'),
                'how_got_power': govt_period.get('how_got_power', 'Unknown'),
                'period_years': f"{year_from}-{year_to}",
                'years_in_power': govt_period.get('number_of_years', 0)
            }
            
            # Map each year in the range to this government period
            for year in range(year_from, year_to + 1):
                if 1700 <= year <= 2025:  # Reasonable range
                    govt_lookup[year] = govt_context
        
        logging.info(f"✓ Created lookup table for {len(govt_lookup)} years")
        return govt_lookup
        
    except Exception as e:
        logging.error(f"✗ Error loading government data: {e}")
        sys.exit(1)

def count_tokens(text):
    """Count tokens in text (words, numbers, punctuation)"""
    if not text or not isinstance(text, str):
        return 0
    
    # Simple tokenization: split by whitespace and count non-empty tokens
    tokens = re.findall(r'\S+', text)
    return len(tokens)

def detect_language(act_data):
    """Detect language based on the first section content"""
    try:
        sections = act_data.get('sections', [])
        if not sections:
            return 'unknown'
        
        # Get text from first section
        first_section = sections[0]
        text_to_analyze = ""
        
        # Collect text from section title and content
        if 'section_title' in first_section:
            text_to_analyze += first_section['section_title'] + " "
        if 'section_content' in first_section:
            text_to_analyze += first_section['section_content']
        
        # If no text in first section, try act title
        if not text_to_analyze.strip():
            text_to_analyze = act_data.get('act_title', '')
        
        if not text_to_analyze.strip():
            return 'unknown'
        
        # Bengali language detection patterns
        bengali_patterns = [
            r'[\u0980-\u09FF]',  # Bengali Unicode range
            r'রহিত',  # "repealed" in Bengali
            r'আইন',   # "law" in Bengali
            r'বিধি',   # "rule" in Bengali
            r'অধ্যায়', # "chapter" in Bengali
            r'ধারা',   # "section" in Bengali
        ]
        
        # English patterns (common legal terms)
        english_patterns = [
            r'\b(Act|Section|Chapter|Rule|Law|Regulation|Ordinance)\b',
            r'\b(whereas|hereby|shall|may|provided|subject)\b',
            r'\b(court|government|authority|minister|president)\b'
        ]
        
        bengali_score = 0
        english_score = 0
        
        # Check for Bengali patterns
        for pattern in bengali_patterns:
            if re.search(pattern, text_to_analyze, re.IGNORECASE):
                bengali_score += 1
        
        # Check for English patterns
        for pattern in english_patterns:
            if re.search(pattern, text_to_analyze, re.IGNORECASE):
                english_score += 1
        
        # Additional check: count Bengali vs Latin characters
        bengali_chars = len(re.findall(r'[\u0980-\u09FF]', text_to_analyze))
        latin_chars = len(re.findall(r'[a-zA-Z]', text_to_analyze))
        
        if bengali_chars > latin_chars:
            bengali_score += 2
        elif latin_chars > bengali_chars:
            english_score += 2
        
        # Determine language
        if bengali_score > english_score:
            return 'bengali'
        elif english_score > bengali_score:
            return 'english'
        else:
            return 'mixed'  # Could be bilingual document
            
    except Exception as e:
        logging.warning(f"Error detecting language: {e}")
        return 'unknown'

def fill_missing_data(act_data):
    """Fill missing data from CSV metadata if available"""
    csv_metadata = act_data.get('csv_metadata', {})
    
    # Fill missing act_title
    if not act_data.get('act_title') or act_data.get('act_title') == 'N/A':
        csv_title = csv_metadata.get('act_title_from_csv')
        if csv_title:
            act_data['act_title'] = csv_title
    
    # Fill missing act_no
    if not act_data.get('act_no') or act_data.get('act_no') == 'N/A':
        csv_act_no = csv_metadata.get('act_no_from_csv')
        if csv_act_no:
            act_data['act_no'] = csv_act_no
    
    # Fill missing act_year
    if not act_data.get('act_year') or act_data.get('act_year') == 'N/A':
        csv_year = csv_metadata.get('act_year_from_csv')
        if csv_year:
            act_data['act_year'] = csv_year
    
    return act_data

def calculate_total_tokens(act_data):
    """Calculate total token count for an act"""
    total_tokens = 0
    
    # Count tokens in main fields
    total_tokens += count_tokens(act_data.get('act_title', ''))
    total_tokens += count_tokens(act_data.get('publication_date', ''))
    
    # Count tokens in sections
    sections = act_data.get('sections', [])
    for section in sections:
        total_tokens += count_tokens(section.get('section_title', ''))
        total_tokens += count_tokens(section.get('section_content', ''))
    
    # Count tokens in footnotes
    footnotes = act_data.get('footnotes', [])
    for footnote in footnotes:
        total_tokens += count_tokens(footnote.get('footnote_text', ''))
    
    # Count tokens in copyright info
    copyright_info = act_data.get('copyright_info', {})
    if isinstance(copyright_info, dict):
        total_tokens += count_tokens(copyright_info.get('copyright_text', ''))
    
    return total_tokens

def extract_year_from_act(act: Dict) -> Optional[int]:
    """Extract year from act data with fallback methods"""
    # Try different sources for year information
    year_sources = [
        act.get('act_year'),
        act.get('csv_metadata', {}).get('act_year_from_csv'),
        act.get('act_no')  # Sometimes year might be in act_no
    ]
    
    for year_source in year_sources:
        if year_source:
            try:
                # Extract 4-digit year from string
                year_str = str(year_source).strip()
                
                # Try direct conversion first
                if year_str.isdigit() and len(year_str) == 4:
                    year = int(year_str)
                    if 1700 <= year <= 2025:  # Reasonable range
                        return year
                
                # Try to find 4-digit year in string
                year_match = re.search(r'\b(1[7-9]\d{2}|20[0-2]\d)\b', year_str)
                if year_match:
                    year = int(year_match.group(1))
                    if 1700 <= year <= 2025:
                        return year
                        
            except (ValueError, TypeError):
                continue
    
    return None

def find_govt_context(act_year: int, govt_lookup: Dict[int, Dict]) -> Dict:
    """Find the appropriate government context for a given year using lookup table"""
    if act_year in govt_lookup:
        return govt_lookup[act_year].copy()
    
    # If no exact match found, return a default context
    return {
        'govt_system': 'Historical period - context not available',
        'position_head_govt': 'Unknown',
        'head_govt_name': 'Unknown',
        'head_govt_designation': 'Unknown',
        'how_got_power': 'Unknown',
        'period_years': f"{act_year}",
        'years_in_power': 0,
        'note': 'Government context not available for this historical period'
    }

def process_single_act_file(file_path: str, govt_lookup: Dict[int, Dict], stats: Dict) -> bool:
    """Process a single act JSON file"""
    try:
        # Load the act data
        with open(file_path, 'r', encoding='utf-8') as f:
            act_data = json.load(f)
        
        # Track original state for statistics
        original_title = act_data.get('act_title', '')
        original_year = act_data.get('act_year', '')
        original_no = act_data.get('act_no', '')
        
        # 1. Fill missing data from CSV metadata
        act_data = fill_missing_data(act_data)
        
        # Track what was filled
        if not original_title or original_title == 'N/A':
            if act_data.get('act_title') and act_data.get('act_title') != 'N/A':
                stats['filled_titles'] += 1
        
        if not original_year or original_year == 'N/A':
            if act_data.get('act_year') and act_data.get('act_year') != 'N/A':
                stats['filled_years'] += 1
        
        if not original_no or original_no == 'N/A':
            if act_data.get('act_no') and act_data.get('act_no') != 'N/A':
                stats['filled_numbers'] += 1
        
        # 2. Calculate token count
        token_count = calculate_total_tokens(act_data)
        act_data['token_count'] = token_count
        stats['token_stats'].append(token_count)
        
        # 3. Detect language
        language = detect_language(act_data)
        act_data['language'] = language
        stats['language_stats'][language] += 1
        
        # 4. Add government context
        act_year = extract_year_from_act(act_data)
        
        if act_year:
            govt_context = find_govt_context(act_year, govt_lookup)
            act_data['government_context'] = govt_context
            stats['acts_with_govt_context'] += 1
            
            # Track government periods used
            govt_system = govt_context.get('govt_system', 'Unknown')
            stats['govt_periods_used'].add(govt_system)
            
            # Year distribution
            year_range = f"{act_year//10*10}s"
            stats['year_distribution'][year_range] = stats['year_distribution'].get(year_range, 0) + 1
        else:
            stats['acts_without_year'] += 1
            # Add a note for acts without determinable year
            act_data['government_context'] = {
                'note': 'Cannot determine enactment year - government context unavailable',
                'govt_system': 'Unknown',
                'position_head_govt': 'Unknown',
                'head_govt_name': 'Unknown',
                'head_govt_designation': 'Unknown',
                'how_got_power': 'Unknown',
                'period_years': 'Unknown',
                'years_in_power': 0
            }
        
        # 5. Add processing metadata
        act_data['processing_info'] = {
            'processed_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'enhanced_with_reducer': True,
            'enhanced_with_govt_context': True,
            'language_detected': language,
            'token_count': token_count
        }
        
        # 6. Save the enhanced act data back to file
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(act_data, f, ensure_ascii=False, indent=2)
        
        stats['processed_files'] += 1
        return True
        
    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
        stats['failed_files'] += 1
        return False

def process_all_act_files(acts_directory: str, govt_lookup: Dict[int, Dict]):
    """Process all JSON files in the acts directory"""
    start_time = time.time()
    
    # Initialize statistics
    stats = {
        'processed_files': 0,
        'failed_files': 0,
        'filled_titles': 0,
        'filled_years': 0,
        'filled_numbers': 0,
        'acts_with_govt_context': 0,
        'acts_without_year': 0,
        'language_stats': Counter(),
        'token_stats': [],
        'govt_periods_used': set(),
        'year_distribution': {}
    }
    
    try:
        # Get all JSON files in acts directory
        if not os.path.exists(acts_directory):
            logging.error(f"Acts directory not found: {acts_directory}")
            return
        
        json_files = [f for f in os.listdir(acts_directory) if f.endswith('.json')]
        total_files = len(json_files)
        
        if total_files == 0:
            logging.warning(f"No JSON files found in {acts_directory}")
            return
        
        logging.info(f"📊 Found {total_files} JSON files to process")
        
        # Process files in batches for memory efficiency
        batch_size = 50
        processed_count = 0
        
        for i in range(0, total_files, batch_size):
            batch_files = json_files[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_files + batch_size - 1) // batch_size
            
            logging.info(f"⏳ Processing batch {batch_num}/{total_batches} ({len(batch_files)} files)")
            
            for filename in batch_files:
                file_path = os.path.join(acts_directory, filename)
                
                # Process the file
                success = process_single_act_file(file_path, govt_lookup, stats)
                
                processed_count += 1
                
                # Progress update every 10 files
                if processed_count % 10 == 0:
                    progress = (processed_count / total_files) * 100
                    elapsed = time.time() - start_time
                    if progress > 0:
                        estimated_total = elapsed / (progress/100)
                        remaining = estimated_total - elapsed
                        logging.info(f"📈 Progress: {progress:.1f}% ({processed_count}/{total_files}) - "
                                   f"Elapsed: {elapsed/60:.1f}m - Remaining: {remaining/60:.1f}m")
            
            # Force garbage collection after each batch
            gc.collect()
        
        # Calculate final statistics
        processing_time = time.time() - start_time
        
        # Token statistics
        token_stats = stats['token_stats']
        avg_tokens = sum(token_stats) / len(token_stats) if token_stats else 0
        min_tokens = min(token_stats) if token_stats else 0
        max_tokens = max(token_stats) if token_stats else 0
        total_tokens = sum(token_stats)
        
        # Final report
        logging.info(f"\n=== PROCESSING SUMMARY ===")
        logging.info(f"📊 File Processing:")
        logging.info(f"   • Total files: {total_files}")
        logging.info(f"   • Successfully processed: {stats['processed_files']}")
        logging.info(f"   • Failed: {stats['failed_files']}")
        
        logging.info(f"📝 Data Enhancement:")
        logging.info(f"   • Titles filled: {stats['filled_titles']}")
        logging.info(f"   • Years filled: {stats['filled_years']}")
        logging.info(f"   • Act numbers filled: {stats['filled_numbers']}")
        
        logging.info(f"🏛️ Government Context:")
        logging.info(f"   • Acts with context: {stats['acts_with_govt_context']}")
        logging.info(f"   • Acts without year: {stats['acts_without_year']}")
        logging.info(f"   • Government periods used: {len(stats['govt_periods_used'])}")
        
        logging.info(f"🌐 Language Distribution:")
        for lang, count in stats['language_stats'].items():
            percentage = (count / stats['processed_files']) * 100 if stats['processed_files'] > 0 else 0
            logging.info(f"   • {lang.capitalize()}: {count} ({percentage:.1f}%)")
        
        logging.info(f"📊 Token Statistics:")
        logging.info(f"   • Total tokens: {total_tokens:,}")
        logging.info(f"   • Average per act: {avg_tokens:.0f}")
        logging.info(f"   • Range: {min_tokens} - {max_tokens}")
        
        if stats['year_distribution']:
            logging.info(f"📈 Year Distribution:")
            for decade, count in sorted(stats['year_distribution'].items()):
                logging.info(f"   • {decade}: {count} acts")
        
        if stats['govt_periods_used']:
            logging.info(f"🏛️ Government Systems Used:")
            for govt_system in sorted(stats['govt_periods_used']):
                logging.info(f"   • {govt_system}")
        
        logging.info(f"⏱️ Processing Time: {processing_time/60:.1f} minutes")
        
    except Exception as e:
        logging.error(f"Error in batch processing: {e}")
        raise

def main():
    """Main function"""
    print("🇧🇩 Individual Act Files - Reducer and Government Context Fusion")
    print("=" * 70)
    
    # File paths
    govt_file = "Data/govt.json"
    acts_directory = "Data/acts"
    
    print(f"📁 Input sources:")
    print(f"   • Government data: {govt_file}")
    print(f"   • Acts directory: {acts_directory}")
    print()
    
    # Load government data and create lookup table
    govt_lookup = load_govt_data(govt_file)
    
    # Process all act files
    process_all_act_files(acts_directory, govt_lookup)
    
    print("\n🎉 Individual act files processing completed successfully!")
    print("💡 Each act file now includes:")
    print("   • Enhanced metadata (filled missing data)")
    print("   • Token counting and language detection")
    print("   • Historical government context")
    print("🔧 Processing was optimized with batch processing for 1500+ files.")

if __name__ == "__main__":
    main()
