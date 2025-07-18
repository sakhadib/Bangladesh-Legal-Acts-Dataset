import json
import os
import glob
import logging
import time
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('combine.log'),
        logging.StreamHandler()
    ]
)

def combine_json_files_optimized(input_dir='acts', output_dir='Data', output_filename='law.json'):
    """
    Memory-optimized combination of JSON files for large datasets (1500+ files)
    
    Args:
        input_dir: Directory containing individual JSON files
        output_dir: Directory to save the combined JSON file
        output_filename: Name of the output file
    """
    start_time = time.time()
    
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Get all JSON files from the input directory
        json_pattern = os.path.join(input_dir, '*.json')
        json_files = glob.glob(json_pattern)
        
        if not json_files:
            logging.warning(f"No JSON files found in {input_dir}")
            return None
        
        total_files = len(json_files)
        logging.info(f"Found {total_files} JSON files in {input_dir}")
        
        # Statistics tracking
        successful_files = 0
        failed_files = 0
        total_sections = 0
        total_footnotes = 0
        
        # Prepare output files
        output_path = os.path.join(output_dir, output_filename)
        meta_path = os.path.join(output_dir, 'meta.json')
        
        # Use streaming JSON writing to avoid loading all data into memory
        with open(output_path, 'w', encoding='utf-8') as output_file:
            # Write opening structure
            output_file.write('{\n  "metadata": {\n')
            
            # We'll update metadata at the end, for now write placeholders
            metadata_pos = output_file.tell()
            
            # Write acts array opening
            output_file.write('  },\n  "acts": [\n')
            
            # Process files in batches to manage memory
            batch_size = 50  # Process 50 files at a time
            acts_written = 0
            
            for i in range(0, total_files, batch_size):
                batch_files = json_files[i:i + batch_size]
                batch_data = []
                
                logging.info(f"Processing batch {i//batch_size + 1}/{(total_files + batch_size - 1)//batch_size} "
                           f"(files {i+1}-{min(i+batch_size, total_files)})")
                
                # Load batch data
                for json_file in batch_files:
                    try:
                        with open(json_file, 'r', encoding='utf-8') as f:
                            act_data = json.load(f)
                        
                        # Add filename and processing info
                        act_data['source_file'] = os.path.basename(json_file)
                        act_data['processing_batch'] = i//batch_size + 1
                        
                        # Collect statistics
                        sections_count = len(act_data.get('sections', []))
                        footnotes_count = len(act_data.get('footnotes', []))
                        total_sections += sections_count
                        total_footnotes += footnotes_count
                        
                        batch_data.append(act_data)
                        successful_files += 1
                        
                    except json.JSONDecodeError as e:
                        logging.error(f"JSON decode error in {json_file}: {e}")
                        failed_files += 1
                    except Exception as e:
                        logging.error(f"Error processing {json_file}: {e}")
                        failed_files += 1
                
                # Sort batch by year for better organization
                def sort_key(act):
                    try:
                        year = act.get('csv_metadata', {}).get('act_year_from_csv', 
                                      act.get('act_year', '0'))
                        try:
                            return int(year)
                        except:
                            return 0
                    except:
                        return 0
                
                batch_data.sort(key=sort_key)
                
                # Write batch to file
                for j, act_data in enumerate(batch_data):
                    if acts_written > 0:
                        output_file.write(',\n')
                    
                    # Write act data with proper indentation
                    json_str = json.dumps(act_data, indent=4, ensure_ascii=False)
                    # Indent the entire JSON object
                    indented_json = '\n'.join('    ' + line for line in json_str.split('\n'))
                    output_file.write(indented_json)
                    acts_written += 1
                
                # Clear batch data from memory
                del batch_data
                
                # Progress update
                progress = (i + batch_size) / total_files * 100
                elapsed = time.time() - start_time
                estimated_total = elapsed / (progress/100) if progress > 0 else 0
                remaining = estimated_total - elapsed
                
                logging.info(f"Progress: {min(progress, 100):.1f}% - "
                           f"Elapsed: {elapsed/60:.1f}m - "
                           f"Remaining: {remaining/60:.1f}m")
            
            # Close acts array and JSON
            output_file.write('\n  ]\n}')
        
        # Now write the metadata at the beginning
        creation_time = time.strftime('%Y-%m-%d %H:%M:%S')
        processing_time = time.time() - start_time
        
        metadata = {
            "total_acts": successful_files,
            "successful_files": successful_files,
            "failed_files": failed_files,
            "total_sections": total_sections,
            "total_footnotes": total_footnotes,
            "source_directory": input_dir,
            "created_timestamp": creation_time,
            "processing_time_minutes": round(processing_time / 60, 2),
            "description": "Combined Bangladesh Laws Database"
        }
        
        # Save metadata separately
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        # Update the main file with correct metadata
        # Read the file, replace metadata section
        with open(output_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Replace the metadata placeholder
        metadata_json = json.dumps(metadata, indent=4, ensure_ascii=False)
        indented_metadata = '\n'.join('    ' + line for line in metadata_json.split('\n')[1:-1])  # Remove outer braces
        
        # Find and replace metadata section
        start_marker = '  "metadata": {\n'
        end_marker = '  },'
        start_pos = content.find(start_marker) + len(start_marker)
        end_pos = content.find(end_marker)
        
        new_content = content[:start_pos] + indented_metadata + '\n  ' + content[end_pos:]
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        # Calculate final file sizes
        file_size = os.path.getsize(output_path)
        file_size_mb = file_size / (1024 * 1024)
        
        meta_size = os.path.getsize(meta_path)
        meta_size_kb = meta_size / 1024
        
        # Final statistics
        logging.info(f"\n=== OPTIMIZED COMBINATION SUMMARY ===")
        logging.info(f"Total JSON files found: {total_files}")
        logging.info(f"Successfully processed: {successful_files}")
        logging.info(f"Failed to process: {failed_files}")
        logging.info(f"Total sections extracted: {total_sections:,}")
        logging.info(f"Total footnotes extracted: {total_footnotes:,}")
        logging.info(f"Processing time: {processing_time/60:.1f} minutes")
        logging.info(f"Output file: {os.path.abspath(output_path)}")
        logging.info(f"Metadata file: {os.path.abspath(meta_path)}")
        logging.info(f"Main file size: {file_size_mb:.2f} MB")
        logging.info(f"Meta file size: {meta_size_kb:.2f} KB")
        logging.info(f"Average file size: {file_size_mb*1024/successful_files:.1f} KB per act")
        
        return output_path, meta_path
        
    except Exception as e:
        logging.error(f"Error in combine_json_files_optimized: {e}")
        return None

def validate_combined_file(file_path):
    """
    Validate the combined JSON file
    """
    try:
        logging.info(f"Validating combined file: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Check structure
        if 'metadata' not in data or 'acts' not in data:
            logging.error("Invalid structure: missing 'metadata' or 'acts' keys")
            return False
        
        acts = data['acts']
        metadata = data['metadata']
        
        logging.info(f"Validation successful:")
        logging.info(f"  - Structure: Valid")
        logging.info(f"  - Total acts: {len(acts)}")
        logging.info(f"  - Metadata total: {metadata.get('total_acts', 'N/A')}")
        logging.info(f"  - Match: {len(acts) == metadata.get('total_acts', 0)}")
        
        return True
        
    except Exception as e:
        logging.error(f"Validation error: {e}")
        return False

if __name__ == "__main__":
    acts_folder_path = 'Data/acts'
    data_folder_path = 'Data'
    
    # Combine all JSON files using optimized method
    result = combine_json_files_optimized(
        input_dir=acts_folder_path, 
        output_dir=data_folder_path
    )
    
    if result:
        output_file, meta_file = result
        # Validate the result
        validate_combined_file(output_file)
        print(f"\nSuccess! Files saved:")
        print(f"  - Combined data: {output_file}")
        print(f"  - Metadata: {meta_file}")
    else:
        print("Failed to combine JSON files")
