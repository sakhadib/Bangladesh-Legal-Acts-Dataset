import json
import re
import logging
import time
from collections import Counter

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reducer.log'),
        logging.StreamHandler()
    ]
)

def count_tokens(text):
    """
    Count tokens in text (words, numbers, punctuation)
    """
    if not text or not isinstance(text, str):
        return 0
    
    # Simple tokenization: split by whitespace and count non-empty tokens
    tokens = re.findall(r'\S+', text)
    return len(tokens)

def detect_language(act_data):
    """
    Detect language based on the first section content
    """
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
    """
    Fill missing data from CSV metadata if available
    """
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
    """
    Calculate total token count for an act
    """
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

def process_law_data(input_file='Data/law.json', output_file='Data/processed_law.json'):
    """
    Process the law.json file with optimizations
    """
    start_time = time.time()
    
    try:
        logging.info(f"Loading data from {input_file}")
        
        # Load the JSON data
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        acts = data.get('acts', [])
        total_acts = len(acts)
        
        logging.info(f"Processing {total_acts} acts...")
        
        # Statistics
        processed_count = 0
        filled_titles = 0
        filled_years = 0
        filled_numbers = 0
        language_stats = Counter()
        token_stats = []
        
        # Process in batches for large datasets
        batch_size = 100
        
        for i in range(0, total_acts, batch_size):
            batch_end = min(i + batch_size, total_acts)
            logging.info(f"Processing batch {i//batch_size + 1}/{(total_acts + batch_size - 1)//batch_size} "
                        f"(acts {i+1}-{batch_end})")
            
            for j in range(i, batch_end):
                act = acts[j]
                
                # Track original state
                original_title = act.get('act_title', '')
                original_year = act.get('act_year', '')
                original_no = act.get('act_no', '')
                
                # 1. Fill missing data from CSV
                act = fill_missing_data(act)
                
                # Track what was filled
                if not original_title or original_title == 'N/A':
                    if act.get('act_title') and act.get('act_title') != 'N/A':
                        filled_titles += 1
                
                if not original_year or original_year == 'N/A':
                    if act.get('act_year') and act.get('act_year') != 'N/A':
                        filled_years += 1
                
                if not original_no or original_no == 'N/A':
                    if act.get('act_no') and act.get('act_no') != 'N/A':
                        filled_numbers += 1
                
                # 2. Calculate token count
                token_count = calculate_total_tokens(act)
                act['token_count'] = token_count
                token_stats.append(token_count)
                
                # 3. Detect language
                language = detect_language(act)
                act['language'] = language
                language_stats[language] += 1
                
                processed_count += 1
            
            # Progress update
            progress = batch_end / total_acts * 100
            elapsed = time.time() - start_time
            if progress > 0:
                estimated_total = elapsed / (progress/100)
                remaining = estimated_total - elapsed
                logging.info(f"Progress: {progress:.1f}% - "
                           f"Elapsed: {elapsed/60:.1f}m - "
                           f"Remaining: {remaining/60:.1f}m")
        
        # Update metadata
        processing_time = time.time() - start_time
        
        # Calculate token statistics
        avg_tokens = sum(token_stats) / len(token_stats) if token_stats else 0
        min_tokens = min(token_stats) if token_stats else 0
        max_tokens = max(token_stats) if token_stats else 0
        total_tokens = sum(token_stats)
        
        # Update metadata
        original_metadata = data.get('metadata', {})
        processed_metadata = {
            **original_metadata,
            "processing_info": {
                "processed_timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                "processing_time_minutes": round(processing_time / 60, 2),
                "filled_missing_titles": filled_titles,
                "filled_missing_years": filled_years,
                "filled_missing_numbers": filled_numbers,
                "language_distribution": dict(language_stats),
                "token_statistics": {
                    "total_tokens": total_tokens,
                    "average_tokens_per_act": round(avg_tokens, 2),
                    "min_tokens": min_tokens,
                    "max_tokens": max_tokens
                }
            }
        }
        
        # Create final data structure
        processed_data = {
            "metadata": processed_metadata,
            "acts": acts
        }
        
        # Save processed data
        logging.info(f"Saving processed data to {output_file}")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, indent=2, ensure_ascii=False)
        
        # Calculate file sizes
        input_size = os.path.getsize(input_file) / (1024 * 1024)
        output_size = os.path.getsize(output_file) / (1024 * 1024)
        
        # Final statistics
        logging.info(f"\n=== PROCESSING SUMMARY ===")
        logging.info(f"Total acts processed: {processed_count}")
        logging.info(f"Missing data filled:")
        logging.info(f"  - Titles: {filled_titles}")
        logging.info(f"  - Years: {filled_years}")
        logging.info(f"  - Act numbers: {filled_numbers}")
        logging.info(f"Language distribution:")
        for lang, count in language_stats.items():
            percentage = (count / total_acts) * 100
            logging.info(f"  - {lang.capitalize()}: {count} ({percentage:.1f}%)")
        logging.info(f"Token statistics:")
        logging.info(f"  - Total tokens: {total_tokens:,}")
        logging.info(f"  - Average per act: {avg_tokens:.0f}")
        logging.info(f"  - Range: {min_tokens} - {max_tokens}")
        logging.info(f"File sizes:")
        logging.info(f"  - Input: {input_size:.2f} MB")
        logging.info(f"  - Output: {output_size:.2f} MB")
        logging.info(f"Processing time: {processing_time/60:.1f} minutes")
        
        return output_file
        
    except Exception as e:
        logging.error(f"Error processing law data: {e}")
        return None

if __name__ == "__main__":
    import os
    
    # Process the law data
    output_file = process_law_data()
    
    if output_file:
        print(f"\nSuccess! Processed file saved as: {output_file}")
    else:
        print("Failed to process law data")
