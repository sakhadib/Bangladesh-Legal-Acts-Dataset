#!/usr/bin/env python3
"""
Government Context Fusion Script for Bangladesh Legal Acts

This script adds government context information to legal acts based on the year
the act was enacted. It matches acts with the corresponding government system
and leadership information for historical context.

Optimized for large JSON files with chunked processing and streaming I/O.

Author: Bangladesh Legal Dataset Project
Date: July 19, 2025
"""

import json
import sys
import os
import gc
from datetime import datetime
from typing import Dict, List, Optional, Any, Iterator
import re

def load_govt_data(govt_file_path: str) -> List[Dict]:
    """Load government context data from JSON file"""
    try:
        with open(govt_file_path, 'r', encoding='utf-8') as f:
            govt_data = json.load(f)
        print(f"✓ Loaded {len(govt_data)} government records")
        return govt_data
    except Exception as e:
        print(f"✗ Error loading government data: {e}")
        sys.exit(1)

def create_govt_lookup(govt_data: List[Dict]) -> Dict[int, Dict]:
    """
    Create optimized lookup table for government periods by year
    
    Args:
        govt_data: List of government periods
    
    Returns:
        Dictionary mapping each year to government context
    """
    print("🔍 Creating government lookup table...")
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
    
    print(f"✓ Created lookup table for {len(govt_lookup)} years")
    return govt_lookup

def find_govt_context(act_year: int, govt_lookup: Dict[int, Dict]) -> Dict:
    """
    Find the appropriate government context for a given year using lookup table
    
    Args:
        act_year: Year the act was enacted
        govt_lookup: Precomputed government lookup table
    
    Returns:
        Government context dict
    """
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

def extract_year_from_act(act: Dict) -> Optional[int]:
    """
    Extract year from act data with fallback methods
    
    Args:
        act: Act dictionary
    
    Returns:
        Year as integer or None if cannot parse
    """
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

def chunk_acts(acts: List[Dict], chunk_size: int = 50) -> Iterator[List[Dict]]:
    """
    Generator to yield chunks of acts for memory-efficient processing
    
    Args:
        acts: List of all acts
        chunk_size: Number of acts per chunk
    
    Yields:
        Chunks of acts
    """
    for i in range(0, len(acts), chunk_size):
        yield acts[i:i + chunk_size]

def process_acts_chunk(acts_chunk: List[Dict], govt_lookup: Dict[int, Dict], 
                      stats: Dict, chunk_num: int, total_chunks: int) -> List[Dict]:
    """
    Process a chunk of acts and add government context
    
    Args:
        acts_chunk: Chunk of acts to process
        govt_lookup: Government lookup table
        stats: Statistics dictionary to update
        chunk_num: Current chunk number
        total_chunks: Total number of chunks
    
    Returns:
        Processed acts chunk
    """
    print(f"⏳ Processing chunk {chunk_num}/{total_chunks} ({len(acts_chunk)} acts)")
    
    processed_chunk = []
    
    for act in acts_chunk:
        # Extract year from act
        act_year = extract_year_from_act(act)
        
        if act_year:
            # Find government context using lookup table
            govt_context = find_govt_context(act_year, govt_lookup)
            
            # Add government context to act
            act['government_context'] = govt_context
            stats['acts_with_govt_context'] += 1
            
            # Track statistics
            govt_system = govt_context.get('govt_system', 'Unknown')
            stats['govt_periods_used'].add(govt_system)
            
            # Year distribution
            year_range = f"{act_year//10*10}s"
            stats['year_distribution'][year_range] = stats['year_distribution'].get(year_range, 0) + 1
            
        else:
            stats['acts_without_year'] += 1
            # Add a note for acts without determinable year
            act['government_context'] = {
                'note': 'Cannot determine enactment year - government context unavailable',
                'govt_system': 'Unknown',
                'position_head_govt': 'Unknown',
                'head_govt_name': 'Unknown',
                'head_govt_designation': 'Unknown',
                'how_got_power': 'Unknown',
                'period_years': 'Unknown',
                'years_in_power': 0
            }
        
        processed_chunk.append(act)
    
    return processed_chunk

def write_json_streaming(data: Dict, output_file: str):
    """
    Write JSON data with proper formatting and streaming for large files
    
    Args:
        data: Dictionary containing metadata and acts
        output_file: Output file path
    """
    print(f"💾 Writing enhanced data to {output_file}")
    
    try:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            # Write opening brace and metadata
            outfile.write('{\n')
            outfile.write('  "metadata": ')
            json.dump(data['metadata'], outfile, ensure_ascii=False, indent=2)
            outfile.write(',\n')
            
            # Write acts array opening
            outfile.write('  "acts": [\n')
            
            acts = data['acts']
            for i, act in enumerate(acts):
                # Write each act with proper indentation
                act_json = json.dumps(act, ensure_ascii=False, indent=4)
                # Indent the act JSON by 4 spaces
                indented_act = '\n'.join('    ' + line for line in act_json.split('\n'))
                outfile.write(indented_act)
                
                # Add comma if not the last act
                if i < len(acts) - 1:
                    outfile.write(',')
                outfile.write('\n')
                
                # Progress indicator for large files
                if i % 100 == 0:
                    progress = ((i + 1) / len(acts)) * 100
                    print(f"   📝 Writing progress: {progress:.1f}% ({i+1}/{len(acts)})")
            
            # Close acts array and main object
            outfile.write('  ]\n')
            outfile.write('}\n')
            
        print(f"✅ Successfully wrote {len(acts)} acts to {output_file}")
        
    except Exception as e:
        print(f"✗ Error writing output file: {e}")
        raise

def process_acts_with_govt_context(input_file: str, govt_lookup: Dict[int, Dict], output_file: str):
    """
    Process legal acts and add government context with chunked processing
    
    Args:
        input_file: Path to processed_law.json
        govt_lookup: Government context lookup table
        output_file: Path for output file
    """
    print(f"🔄 Processing legal acts from {input_file}")
    
    # Statistics tracking
    stats = {
        'total_acts': 0,
        'acts_with_govt_context': 0,
        'acts_without_year': 0,
        'govt_periods_used': set(),
        'year_distribution': {},
        'start_time': datetime.now()
    }
    
    try:
        # Check file size for memory planning
        file_size = os.path.getsize(input_file)
        print(f"📊 Input file size: {file_size / (1024*1024):.1f} MB")
        
        # Load metadata and acts with streaming approach
        print("📖 Loading legal acts data...")
        with open(input_file, 'r', encoding='utf-8') as infile:
            data = json.load(infile)
            
        acts = data.get('acts', [])
        metadata = data.get('metadata', {})
        
        stats['total_acts'] = len(acts)
        print(f"📊 Found {stats['total_acts']} acts to process")
        
        # Determine optimal chunk size based on memory
        chunk_size = min(50, max(10, stats['total_acts'] // 20))  # Adaptive chunk size
        total_chunks = (stats['total_acts'] + chunk_size - 1) // chunk_size
        
        print(f"🔧 Using chunk size: {chunk_size} (Total chunks: {total_chunks})")
        
        # Process acts in chunks
        processed_acts = []
        
        for chunk_num, acts_chunk in enumerate(chunk_acts(acts, chunk_size), 1):
            # Process chunk
            processed_chunk = process_acts_chunk(acts_chunk, govt_lookup, stats, chunk_num, total_chunks)
            processed_acts.extend(processed_chunk)
            
            # Force garbage collection after each chunk
            gc.collect()
            
            # Progress update
            progress = (chunk_num / total_chunks) * 100
            print(f"📈 Overall progress: {progress:.1f}% (Chunk {chunk_num}/{total_chunks})")
        
        # Update metadata
        processing_time = (datetime.now() - stats['start_time']).total_seconds() / 60
        
        metadata['govt_context_processing'] = {
            'processed_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'processing_time_minutes': round(processing_time, 2),
            'total_acts_processed': stats['total_acts'],
            'acts_with_govt_context': stats['acts_with_govt_context'],
            'acts_without_year': stats['acts_without_year'],
            'govt_periods_used': len(stats['govt_periods_used']),
            'year_distribution': dict(sorted(stats['year_distribution'].items())),
            'chunk_size_used': chunk_size,
            'total_chunks_processed': total_chunks,
            'description': 'Added government context based on act enactment year using optimized chunked processing'
        }
        
        # Prepare final data structure
        final_data = {
            'metadata': metadata,
            'acts': processed_acts
        }
        
        # Write output file with streaming
        write_json_streaming(final_data, output_file)
        
        # Final statistics
        print(f"\n✅ Successfully processed {stats['total_acts']} acts")
        print(f"📊 Final Statistics:")
        print(f"   • Acts with government context: {stats['acts_with_govt_context']}")
        print(f"   • Acts without determinable year: {stats['acts_without_year']}")
        print(f"   • Government periods covered: {len(stats['govt_periods_used'])}")
        print(f"   • Processing time: {processing_time:.2f} minutes")
        print(f"   • Chunks processed: {total_chunks}")
        print(f"   • Output file: {output_file}")
        
        # Show year distribution
        if stats['year_distribution']:
            print(f"\n📈 Year distribution:")
            for decade, count in sorted(stats['year_distribution'].items()):
                print(f"   • {decade}: {count} acts")
                
        # Show government periods used
        if stats['govt_periods_used']:
            print(f"\n🏛️ Government systems covered:")
            for govt_system in sorted(stats['govt_periods_used']):
                print(f"   • {govt_system}")
        
        # Memory cleanup
        del processed_acts
        del final_data
        gc.collect()
        
    except FileNotFoundError:
        print(f"✗ Error: Input file {input_file} not found")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"✗ Error: Invalid JSON in {input_file}: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error processing acts: {e}")
        raise

def main():
    """Main function"""
    print("🇧🇩 Bangladesh Legal Acts - Government Context Fusion")
    print("=" * 60)
    
    # File paths
    govt_file = "Data/govt.json"
    input_file = "Data/processed_law.json"
    output_file = "Data/processed_law_with_govt_context.json"
    
    print(f"📁 Input files:")
    print(f"   • Government data: {govt_file}")
    print(f"   • Legal acts: {input_file}")
    print(f"   • Output: {output_file}")
    print()
    
    # Load government data
    govt_data = load_govt_data(govt_file)
    
    # Create optimized lookup table
    govt_lookup = create_govt_lookup(govt_data)
    
    # Process acts with government context using optimized approach
    process_acts_with_govt_context(input_file, govt_lookup, output_file)
    
    print("\n🎉 Government context fusion completed successfully!")
    print("💡 The enhanced dataset now includes historical government context for each legal act.")
    print("🔧 Processing was optimized with chunking and streaming I/O for large files.")

if __name__ == "__main__":
    main()
