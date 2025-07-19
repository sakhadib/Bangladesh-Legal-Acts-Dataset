#!/usr/bin/env python3
"""
Final Combination Script for Bangladesh Legal Acts
Combines all processed JSON files into a single comprehensive dataset.
"""

import json
import os
import logging
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import gc
import codecs

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('finalCombine.log'),
        logging.StreamHandler()
    ]
)

def load_json_file(file_path):
    """Load a JSON file safely with proper encoding handling."""
    try:
        # First try to detect encoding
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

def combine_all_acts():
    """Combine all JSON files from Data/acts/ into a single comprehensive dataset."""
    
    # Setup paths
    acts_dir = Path("Data/acts")
    output_file = Path("Data/Contextualized_Bangladesh_Legal_Acts.json")
    
    if not acts_dir.exists():
        logging.error(f"Acts directory not found: {acts_dir}")
        return False
    
    # Get all JSON files
    json_files = list(acts_dir.glob("*.json"))
    total_files = len(json_files)
    
    if total_files == 0:
        logging.error("No JSON files found in acts directory")
        return False
    
    logging.info(f"Found {total_files} JSON files to combine")
    
    # Initialize combined dataset structure
    combined_data = {
        "dataset_info": {
            "name": "Contextualized Bangladesh Legal Acts Dataset",
            "description": "Comprehensive dataset of Bangladesh legal acts with government context, legal system context, and enhanced metadata",
            "version": "1.0",
            "created_date": datetime.now().isoformat(),
            "total_acts": 0,
            "data_sources": [
                "Bangladesh Government Legal Database",
                "Historical Government Context Data",
                "Legal System Contextual Information"
            ],
            "processing_pipeline": [
                "Legal Act Scraping",
                "Data Cleaning and Structuring", 
                "Government Context Integration",
                "Legal System Context Addition",
                "Year Translation (Bengali to English)",
                "Token Count Calculation",
                "Final Dataset Compilation"
            ],
            "features": [
                "Act title, number, and year",
                "Full legal text sections",
                "Historical government context",
                "Legal system framework context",
                "Processing metadata",
                "Token counts for content analysis"
            ]
        },
        "acts": []
    }
    
    # Statistics tracking
    stats = {
        "total_processed": 0,
        "successful": 0,
        "errors": 0,
        "total_tokens": 0,
        "years_range": {"min": float('inf'), "max": 0},
        "government_contexts": set(),
        "legal_systems": set()
    }
    
    # Process files in chunks for memory efficiency
    chunk_size = 50
    
    for i in tqdm(range(0, total_files, chunk_size), desc="Processing chunks"):
        chunk_files = json_files[i:i + chunk_size]
        
        for file_path in tqdm(chunk_files, desc=f"Chunk {i//chunk_size + 1}", leave=False):
            try:
                act_data = load_json_file(file_path)
                
                if act_data is None:
                    stats["errors"] += 1
                    continue
                
                # Add file source info
                act_data["source_file"] = file_path.name
                
                # Update statistics
                if "act_year" in act_data and act_data["act_year"]:
                    try:
                        year = int(act_data["act_year"])
                        stats["years_range"]["min"] = min(stats["years_range"]["min"], year)
                        stats["years_range"]["max"] = max(stats["years_range"]["max"], year)
                    except:
                        pass
                
                # Track government contexts
                if "government_context" in act_data and act_data["government_context"]:
                    if "government_name" in act_data["government_context"]:
                        gov_name = act_data["government_context"]["government_name"]
                        if isinstance(gov_name, str):
                            stats["government_contexts"].add(gov_name)
                
                # Track legal systems
                if "legal_system_context" in act_data and act_data["legal_system_context"]:
                    if not isinstance(act_data["legal_system_context"], str):  # Not an error message
                        if "legal_framework" in act_data["legal_system_context"]:
                            legal_framework = act_data["legal_system_context"]["legal_framework"]
                            if isinstance(legal_framework, str):
                                stats["legal_systems"].add(legal_framework)
                
                # Add token count to total
                if "token_count" in act_data:
                    try:
                        stats["total_tokens"] += int(act_data["token_count"])
                    except:
                        pass
                
                combined_data["acts"].append(act_data)
                stats["successful"] += 1
                
            except Exception as e:
                logging.error(f"Error processing {file_path}: {e}")
                stats["errors"] += 1
            
            stats["total_processed"] += 1
        
        # Garbage collection after each chunk
        gc.collect()
    
    # Update dataset info with statistics
    combined_data["dataset_info"]["total_acts"] = stats["successful"]
    combined_data["dataset_info"]["processing_stats"] = {
        "total_files_processed": stats["total_processed"],
        "successful_integrations": stats["successful"],
        "failed_integrations": stats["errors"],
        "total_tokens": stats["total_tokens"],
        "year_range": {
            "earliest": stats["years_range"]["min"] if stats["years_range"]["min"] != float('inf') else None,
            "latest": stats["years_range"]["max"] if stats["years_range"]["max"] > 0 else None
        },
        "unique_governments": len(stats["government_contexts"]),
        "unique_legal_systems": len(stats["legal_systems"])
    }
    
    # Sort acts by year for better organization
    try:
        combined_data["acts"].sort(key=lambda x: int(x.get("act_year", 0)))
    except:
        logging.warning("Could not sort acts by year")
    
    # Write combined dataset
    logging.info(f"Writing combined dataset to {output_file}")
    
    try:
        # Ensure output directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(combined_data, f, ensure_ascii=False, indent=2)
        
        # Log final statistics
        logging.info("=== FINAL COMBINATION COMPLETE ===")
        logging.info(f"Total acts processed: {stats['total_processed']}")
        logging.info(f"Successfully combined: {stats['successful']}")
        logging.info(f"Errors encountered: {stats['errors']}")
        logging.info(f"Total tokens: {stats['total_tokens']:,}")
        logging.info(f"Year range: {stats['years_range']['min']} - {stats['years_range']['max']}")
        logging.info(f"Unique governments: {len(stats['government_contexts'])}")
        logging.info(f"Unique legal systems: {len(stats['legal_systems'])}")
        logging.info(f"Output file: {output_file}")
        logging.info(f"File size: {output_file.stat().st_size / (1024*1024):.2f} MB")
        
        print(f"\n✅ Successfully created combined dataset!")
        print(f"📁 File: {output_file}")
        print(f"📊 Total Acts: {stats['successful']:,}")
        print(f"🔤 Total Tokens: {stats['total_tokens']:,}")
        print(f"📅 Year Range: {stats['years_range']['min']} - {stats['years_range']['max']}")
        print(f"💾 File Size: {output_file.stat().st_size / (1024*1024):.2f} MB")
        
        return True
        
    except Exception as e:
        logging.error(f"Error writing combined file: {e}")
        return False

def main():
    """Main execution function."""
    print("🚀 Starting Final Dataset Combination...")
    print("=" * 50)
    
    success = combine_all_acts()
    
    if success:
        print("\n🎉 Dataset combination completed successfully!")
    else:
        print("\n❌ Dataset combination failed. Check logs for details.")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
