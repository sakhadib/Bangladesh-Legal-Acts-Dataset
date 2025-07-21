#!/usr/bin/env python3
"""
JSON to CSV Conversion Script for Bangladesh Legal Acts
Converts all processed JSON files into a comprehensive CSV format with efficient mapping.
"""

import json
import csv
import os
import logging
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import pandas as pd
import gc

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('jsonToCSV.log'),
        logging.StreamHandler()
    ]
)

def load_json_file(file_path):
    """Load a JSON file safely with proper encoding handling."""
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read()
        
        # Try UTF-8 first (most common)
        try:
            content = raw_data.decode('utf-8')
        except UnicodeDecodeError:
            # If UTF-8 fails, try other encodings
            for encoding in ['utf-8-sig', 'latin1', 'cp1252', 'iso-8859-1']:
                try:
                    content = raw_data.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                # If all fail, use utf-8 with error handling
                content = raw_data.decode('utf-8', errors='replace')
        
        # Clean up any problematic characters
        content = content.replace('\ufeff', '')  # Remove BOM
        
        return json.loads(content)
        
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error in {file_path}: {e}")
        return None
    except Exception as e:
        logging.error(f"Error loading {file_path}: {e}")
        return None

def extract_text_content(sections):
    """Extract and combine all section content into a single text field."""
    if not sections or not isinstance(sections, list):
        return ""
    
    text_parts = []
    for section in sections:
        if isinstance(section, dict) and "section_content" in section:
            content = section["section_content"]
            if content:
                text_parts.append(str(content).strip())
    
    return " | ".join(text_parts)

def extract_footnotes_text(footnotes):
    """Extract and combine all footnote text."""
    if not footnotes or not isinstance(footnotes, list):
        return ""
    
    footnote_parts = []
    for footnote in footnotes:
        if isinstance(footnote, dict) and "footnote_text" in footnote:
            text = footnote["footnote_text"]
            if text:
                footnote_parts.append(str(text).strip())
    
    return " | ".join(footnote_parts)

def flatten_government_context(gov_context):
    """Flatten government context into separate columns."""
    if not gov_context or isinstance(gov_context, str):
        return {
            'gov_system': '',
            'position_head_govt': '',
            'head_govt_name': '',
            'head_govt_designation': '',
            'how_got_power': '',
            'period_years': '',
            'years_in_power': ''
        }
    
    return {
        'gov_system': str(gov_context.get('govt_system', '')),
        'position_head_govt': str(gov_context.get('position_head_govt', '')),
        'head_govt_name': str(gov_context.get('head_govt_name', '')),
        'head_govt_designation': str(gov_context.get('head_govt_designation', '')),
        'how_got_power': str(gov_context.get('how_got_power', '')),
        'period_years': str(gov_context.get('period_years', '')),
        'years_in_power': str(gov_context.get('years_in_power', ''))
    }

def flatten_legal_context(legal_context):
    """Flatten legal system context into separate columns."""
    if not legal_context or isinstance(legal_context, str):
        return {
            'legal_framework': '',
            'legal_period': '',
            'court_system': '',
            'policing_system': '',
            'land_tenure_system': '',
            'legal_characteristics': ''
        }
    
    # Handle nested structure
    legal_framework = ''
    legal_period = ''
    court_system = ''
    policing_system = ''
    land_tenure = ''
    characteristics = ''
    
    if isinstance(legal_context, dict):
        # Extract period info
        if 'period_info' in legal_context:
            period_info = legal_context['period_info']
            if isinstance(period_info, dict):
                legal_period = f"{period_info.get('period_name', '')} ({period_info.get('year_range', '')})"
        
        # Extract legal framework
        if 'legal_framework' in legal_context:
            framework = legal_context['legal_framework']
            if isinstance(framework, dict):
                primary_laws = framework.get('primary_laws', [])
                if isinstance(primary_laws, list):
                    legal_framework = "; ".join(primary_laws)
                
                courts = framework.get('court_system', [])
                if isinstance(courts, list):
                    court_system = "; ".join(courts)
            elif isinstance(framework, str):
                legal_framework = framework
        
        # Extract policing system
        if 'policing_system' in legal_context:
            policing = legal_context['policing_system']
            if isinstance(policing, dict):
                law_enforcement = policing.get('law_enforcement', '')
                military_police = policing.get('military_police', '')
                policing_system = f"{law_enforcement}; {military_police}".strip('; ')
        
        # Extract land relations
        if 'land_relations' in legal_context:
            land_rel = legal_context['land_relations']
            if isinstance(land_rel, dict):
                tenure_system = land_rel.get('tenure_system', '')
                property_rights = land_rel.get('property_rights', '')
                land_tenure = f"{tenure_system}; {property_rights}".strip('; ')
        
        # Extract key characteristics
        if 'key_characteristics' in legal_context:
            chars = legal_context['key_characteristics']
            if isinstance(chars, list):
                characteristics = "; ".join(chars)
    
    return {
        'legal_framework': legal_framework,
        'legal_period': legal_period,
        'court_system': court_system,
        'policing_system': policing_system,
        'land_tenure_system': land_tenure,
        'legal_characteristics': characteristics
    }

def convert_json_to_csv_row(json_data, file_name):
    """Convert a single JSON file to a CSV row."""
    try:
        # Basic information
        row = {
            'source_file': file_name,
            'act_title': str(json_data.get('act_title', '')),
            'act_no': str(json_data.get('act_no', '')),
            'act_year': str(json_data.get('act_year', '')),
            'publication_date': str(json_data.get('publication_date', '')),
            'language': str(json_data.get('language', '')),
            'token_count': str(json_data.get('token_count', '')),
            'is_repealed': str(json_data.get('is_repealed', '')),
            'source_url': str(json_data.get('source_url', '')),
            'fetch_timestamp': str(json_data.get('fetch_timestamp', ''))
        }
        
        # Extract text content
        sections = json_data.get('sections', [])
        row['full_text_content'] = extract_text_content(sections)
        row['section_count'] = str(len(sections)) if sections else '0'
        
        # Extract footnotes
        footnotes = json_data.get('footnotes', [])
        row['footnotes_text'] = extract_footnotes_text(footnotes)
        row['footnote_count'] = str(len(footnotes)) if footnotes else '0'
        
        # Flatten government context
        gov_context = flatten_government_context(json_data.get('government_context'))
        row.update(gov_context)
        
        # Flatten legal context
        legal_context = flatten_legal_context(json_data.get('legal_system_context'))
        row.update(legal_context)
        
        # Processing information
        processing_info = json_data.get('processing_info', {})
        if isinstance(processing_info, dict):
            row['enhanced_with_reducer'] = str(processing_info.get('enhanced_with_reducer', ''))
            row['enhanced_with_govt_context'] = str(processing_info.get('enhanced_with_govt_context', ''))
            row['legal_context_added'] = str(processing_info.get('legal_context_added', ''))
            row['year_standardized'] = str(processing_info.get('year_standardized', ''))
            row['token_count_updated'] = str(processing_info.get('token_count_updated', ''))
        else:
            row['enhanced_with_reducer'] = ''
            row['enhanced_with_govt_context'] = ''
            row['legal_context_added'] = ''
            row['year_standardized'] = ''
            row['token_count_updated'] = ''
        
        # CSV metadata
        csv_metadata = json_data.get('csv_metadata', {})
        if isinstance(csv_metadata, dict):
            row['csv_act_title'] = str(csv_metadata.get('act_title_from_csv', ''))
            row['csv_act_no'] = str(csv_metadata.get('act_no_from_csv', ''))
            row['csv_act_year'] = str(csv_metadata.get('act_year_from_csv', ''))
            row['csv_is_repealed'] = str(csv_metadata.get('is_repealed', ''))
        else:
            row['csv_act_title'] = ''
            row['csv_act_no'] = ''
            row['csv_act_year'] = ''
            row['csv_is_repealed'] = ''
        
        # Copyright info
        copyright_info = json_data.get('copyright_info', {})
        if isinstance(copyright_info, dict):
            row['copyright_text'] = str(copyright_info.get('copyright_text', ''))
        else:
            row['copyright_text'] = ''
        
        return row
        
    except Exception as e:
        logging.error(f"Error converting {file_name} to CSV row: {e}")
        return None

def convert_all_json_to_csv():
    """Convert all JSON files to a single CSV file."""
    
    # Setup paths
    acts_dir = Path("Data/acts")
    output_file = Path("Data/Bangladesh_Legal_Acts_Dataset.csv")
    
    if not acts_dir.exists():
        logging.error(f"Acts directory not found: {acts_dir}")
        return False
    
    # Get all JSON files
    json_files = list(acts_dir.glob("*.json"))
    total_files = len(json_files)
    
    if total_files == 0:
        logging.error("No JSON files found in acts directory")
        return False
    
    logging.info(f"Found {total_files} JSON files to convert")
    
    # Define CSV columns
    csv_columns = [
        'source_file', 'act_title', 'act_no', 'act_year', 'publication_date',
        'language', 'token_count', 'is_repealed', 'source_url', 'fetch_timestamp',
        'full_text_content', 'section_count', 'footnotes_text', 'footnote_count',
        'gov_system', 'position_head_govt', 'head_govt_name', 'head_govt_designation',
        'how_got_power', 'period_years', 'years_in_power',
        'legal_framework', 'legal_period', 'court_system', 'policing_system',
        'land_tenure_system', 'legal_characteristics',
        'enhanced_with_reducer', 'enhanced_with_govt_context', 'legal_context_added',
        'year_standardized', 'token_count_updated',
        'csv_act_title', 'csv_act_no', 'csv_act_year', 'csv_is_repealed',
        'copyright_text'
    ]
    
    # Statistics tracking
    stats = {
        "total_processed": 0,
        "successful": 0,
        "errors": 0,
        "total_tokens": 0,
        "years_range": {"min": float('inf'), "max": 0}
    }
    
    # Process files in chunks and write to CSV
    chunk_size = 100
    all_rows = []
    
    logging.info("Processing JSON files...")
    
    for i in tqdm(range(0, total_files, chunk_size), desc="Processing chunks"):
        chunk_files = json_files[i:i + chunk_size]
        chunk_rows = []
        
        for file_path in tqdm(chunk_files, desc=f"Chunk {i//chunk_size + 1}", leave=False):
            try:
                json_data = load_json_file(file_path)
                
                if json_data is None:
                    stats["errors"] += 1
                    stats["total_processed"] += 1
                    continue
                
                # Convert to CSV row
                csv_row = convert_json_to_csv_row(json_data, file_path.name)
                
                if csv_row is None:
                    stats["errors"] += 1
                    stats["total_processed"] += 1
                    continue
                
                chunk_rows.append(csv_row)
                
                # Update statistics
                if csv_row['act_year']:
                    try:
                        year = int(csv_row['act_year'])
                        stats["years_range"]["min"] = min(stats["years_range"]["min"], year)
                        stats["years_range"]["max"] = max(stats["years_range"]["max"], year)
                    except:
                        pass
                
                if csv_row['token_count']:
                    try:
                        stats["total_tokens"] += int(csv_row['token_count'])
                    except:
                        pass
                
                stats["successful"] += 1
                stats["total_processed"] += 1
                
            except Exception as e:
                logging.error(f"Error processing {file_path}: {e}")
                stats["errors"] += 1
                stats["total_processed"] += 1
        
        # Add chunk rows to all rows
        all_rows.extend(chunk_rows)
        
        # Garbage collection after each chunk
        gc.collect()
    
    # Write to CSV file
    logging.info(f"Writing CSV file to {output_file}")
    
    try:
        # Ensure output directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Write CSV using pandas for better handling of special characters
        df = pd.DataFrame(all_rows, columns=csv_columns)
        
        # Sort by year for better organization
        try:
            df['act_year_int'] = pd.to_numeric(df['act_year'], errors='coerce')
            df = df.sort_values('act_year_int', na_position='last')
            df = df.drop('act_year_int', axis=1)
        except:
            logging.warning("Could not sort by year")
        
        # Write to CSV with proper encoding
        df.to_csv(output_file, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
        
        # Log final statistics
        logging.info("=== CSV CONVERSION COMPLETE ===")
        logging.info(f"Total files processed: {stats['total_processed']}")
        logging.info(f"Successfully converted: {stats['successful']}")
        logging.info(f"Errors encountered: {stats['errors']}")
        logging.info(f"Total tokens: {stats['total_tokens']:,}")
        logging.info(f"Year range: {stats['years_range']['min']} - {stats['years_range']['max']}")
        logging.info(f"Output file: {output_file}")
        logging.info(f"File size: {output_file.stat().st_size / (1024*1024):.2f} MB")
        
        print(f"\n✅ Successfully created CSV dataset!")
        print(f"📁 File: {output_file}")
        print(f"📊 Total Acts: {stats['successful']:,}")
        print(f"🔤 Total Tokens: {stats['total_tokens']:,}")
        print(f"📅 Year Range: {stats['years_range']['min']} - {stats['years_range']['max']}")
        print(f"💾 File Size: {output_file.stat().st_size / (1024*1024):.2f} MB")
        print(f"📈 Success Rate: {(stats['successful']/stats['total_processed']*100):.1f}%")
        
        return True
        
    except Exception as e:
        logging.error(f"Error writing CSV file: {e}")
        return False

def create_csv_metadata():
    """Create a metadata file describing the CSV structure."""
    
    metadata = {
        "dataset_name": "Bangladesh Legal Acts Dataset - CSV Format",
        "version": "1.0",
        "created_date": datetime.now().isoformat(),
        "description": "Comprehensive CSV dataset of Bangladesh legal acts with flattened structure",
        "total_columns": 34,
        "column_descriptions": {
            "source_file": "Original JSON filename",
            "act_title": "Title of the legal act",
            "act_no": "Act number/identifier",
            "act_year": "Year of enactment",
            "publication_date": "Date of publication",
            "language": "Language of the act (english/bengali/mixed)",
            "token_count": "Total word tokens in the act",
            "is_repealed": "Whether the act has been repealed",
            "source_url": "URL of the original source",
            "fetch_timestamp": "When the data was scraped",
            "full_text_content": "Complete text content of all sections (pipe-separated)",
            "section_count": "Number of sections in the act",
            "footnotes_text": "All footnote text (pipe-separated)",
            "footnote_count": "Number of footnotes",
            "gov_system": "Government system at time of enactment",
            "position_head_govt": "Position title of head of government",
            "head_govt_name": "Name of head of government",
            "head_govt_designation": "Official designation",
            "how_got_power": "How the government came to power",
            "period_years": "Years the government was in power",
            "years_in_power": "Duration in years",
            "legal_framework": "Legal framework description",
            "legal_period": "Legal system period",
            "court_system": "Court system structure",
            "policing_system": "Policing system description",
            "land_tenure_system": "Land tenure and property system",
            "legal_characteristics": "Key legal characteristics",
            "enhanced_with_reducer": "Whether enhanced with reducer script",
            "enhanced_with_govt_context": "Whether government context was added",
            "legal_context_added": "Whether legal system context was added",
            "year_standardized": "Whether year was standardized",
            "token_count_updated": "Whether token count was updated",
            "csv_act_title": "Act title from original CSV",
            "csv_act_no": "Act number from original CSV",
            "csv_act_year": "Act year from original CSV",
            "csv_is_repealed": "Repealed status from original CSV",
            "copyright_text": "Copyright information"
        }
    }
    
    metadata_file = Path("Data/CSV_Dataset_Metadata.json")
    
    try:
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logging.info(f"CSV metadata created: {metadata_file}")
        return True
        
    except Exception as e:
        logging.error(f"Error creating CSV metadata: {e}")
        return False

def main():
    """Main execution function."""
    print("🚀 Starting JSON to CSV Conversion...")
    print("=" * 50)
    
    success = convert_all_json_to_csv()
    
    if success:
        print("\n📋 Creating CSV metadata...")
        create_csv_metadata()
        print("\n🎉 JSON to CSV conversion completed successfully!")
        print("\nFiles created:")
        print("📄 Bangladesh_Legal_Acts_Dataset.csv - Main dataset")
        print("📋 CSV_Dataset_Metadata.json - Column descriptions")
    else:
        print("\n❌ JSON to CSV conversion failed. Check logs for details.")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
