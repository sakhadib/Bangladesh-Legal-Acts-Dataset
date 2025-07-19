import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import os
from urllib.parse import urlparse
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('act_fetcher.log'),
        logging.StreamHandler()
    ]
)

def extract_act_data_from_html(soup, url):
    """
    Extract act data from BeautifulSoup object and return structured data
    """
    try:
        act_data = {}
        
        # Extract Act Title (from <h3> tag)
        title_tag = soup.find('h3')
        act_data['act_title'] = title_tag.get_text(strip=True) if title_tag else 'N/A'
        
        # Extract Act Number and Year (from <h4> tag)
        h4_tag = soup.find('h4')
        if h4_tag:
            act_no_year = h4_tag.get_text(strip=True)
            parts = act_no_year.split(' ')
            act_data['act_no'] = parts[2] if len(parts) > 2 else 'N/A'
            act_data['act_year'] = parts[4] if len(parts) > 4 else 'N/A'
        else:
            act_data['act_no'] = 'N/A'
            act_data['act_year'] = 'N/A'
        
        # Extract the Publication Date (from <div id="date">)
        date_tag = soup.find('div', id='date')
        act_data['publication_date'] = date_tag.get_text(strip=True) if date_tag else 'N/A'
        
        # Extract the Sections of the Act (from <div class="row lineremoves">)
        sections = []
        for section in soup.find_all('div', class_='row lineremoves'):
            section_data = {}
            
            # Extract the section title (from <p class="act-chapter-name">)
            section_title = section.find('p', class_='act-chapter-name')
            if section_title:
                section_data['section_title'] = section_title.get_text(strip=True)
            
            # Extract the section content (from <div class="col-sm-9 txt-details">)
            section_content = section.find('div', class_='col-sm-9 txt-details')
            if section_content:
                section_data['section_content'] = section_content.get_text(strip=True)
            
            # Only add section if it has content
            if section_data:
                sections.append(section_data)
        
        act_data['sections'] = sections
        
        # Extract Footnotes (from <ul class="footnoteListAll">)
        footnotes = []
        for footnote in soup.find_all('li', class_='footnoteList'):
            footnote_data = {}
            footnote_data['footnote_text'] = footnote.get_text(strip=True)
            footnotes.append(footnote_data)
        
        act_data['footnotes'] = footnotes
        
        # Extract the Copyright and Ministry Information (from .copy-right section)
        copyright_info = {}
        copyright_info_section = soup.find('section', class_='copy-right')
        if copyright_info_section:
            copyright_info['copyright_text'] = copyright_info_section.get_text(strip=True)
        
        act_data['copyright_info'] = copyright_info
        act_data['source_url'] = url
        act_data['fetch_timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')
        
        return act_data
        
    except Exception as e:
        logging.error(f"Error extracting data from HTML: {e}")
        return None

def fetch_and_save_act(row, output_dir, delay: float = 2):
    """
    Fetch a single act from URL and save as JSON
    """
    act_title = 'Unknown'
    full_text_url = 'Unknown'
    
    try:
        act_title = row['Act Title']
        act_no = row['Act No']
        act_year = row['Act Year']
        full_text_url = row['full_text']
        is_repealed = row['is_repealed']
        
        # Create filename from act number or title
        # Extract act ID from URL for filename
        parsed_url = urlparse(full_text_url)
        filename_base = parsed_url.path.split('/')[-1].replace('.html', '')
        filename = f"{filename_base}.json"
        filepath = os.path.join(output_dir, filename)
        
        # Skip if file already exists
        if os.path.exists(filepath):
            logging.info(f"File {filename} already exists, skipping...")
            return True
        
        logging.info(f"Fetching: {act_title} ({act_year}) - {full_text_url}")
        
        # Make HTTP request with timeout
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(full_text_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract data
        act_data = extract_act_data_from_html(soup, full_text_url)
        
        if act_data:
            # Add metadata from CSV
            act_data['csv_metadata'] = {
                'act_title_from_csv': act_title,
                'act_no_from_csv': act_no,
                'act_year_from_csv': act_year,
                'is_repealed': is_repealed
            }
            
            # Save as JSON
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(act_data, f, indent=4, ensure_ascii=False)
            
            logging.info(f"Successfully saved: {filename}")
            
            # Delay to be respectful to server
            time.sleep(delay)
            return True
        else:
            logging.error(f"Failed to extract data from: {full_text_url}")
            return False
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error fetching {full_text_url}: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error processing {act_title} - {full_text_url}: {e}")
        return False

def fetch_acts_from_csv(csv_file='filtered_act_list.csv', output_dir='acts', limit=None, delay: float = 2.0):
    """
    Main function to fetch acts from CSV file
    
    Args:
        csv_file: Path to the CSV file
        output_dir: Directory to save JSON files
        limit: Number of acts to fetch (None for all)
        delay: Delay between requests in seconds
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Read CSV file
        df = pd.read_csv(csv_file)
        logging.info(f"Loaded {len(df)} acts from {csv_file}")
        
        # Apply limit if specified
        if limit:
            df = df.head(limit)
            logging.info(f"Processing first {limit} acts for testing")
        
        # Statistics
        total_acts = len(df)
        successful = 0
        failed = 0
        skipped = 0
        
        # Process each act
        for idx, (index, row) in enumerate(df.iterrows()):
            try:
                logging.info(f"Processing {idx + 1}/{total_acts}")
                
                # Check if file already exists
                parsed_url = urlparse(row['full_text'])
                filename_base = parsed_url.path.split('/')[-1].replace('.html', '')
                filename = f"{filename_base}.json"
                filepath = os.path.join(output_dir, filename)
                
                if os.path.exists(filepath):
                    logging.info(f"File {filename} already exists, skipping...")
                    skipped += 1
                    continue
                
                success = fetch_and_save_act(row, output_dir, delay)
                
                if success:
                    successful += 1
                else:
                    failed += 1
                    
            except KeyboardInterrupt:
                logging.info("Process interrupted by user")
                break
            except Exception as e:
                logging.error(f"Error processing row {idx}: {e}")
                failed += 1
                continue
        
        # Final statistics
        logging.info(f"\n=== FETCH SUMMARY ===")
        logging.info(f"Total acts processed: {total_acts}")
        logging.info(f"Successfully fetched: {successful}")
        logging.info(f"Failed: {failed}")
        logging.info(f"Skipped (already exists): {skipped}")
        logging.info(f"Files saved in: {os.path.abspath(output_dir)}")
        
    except Exception as e:
        logging.error(f"Error in main fetch function: {e}")

if __name__ == "__main__":
    # Test with first 3 acts
    # print("Testing with first 3 acts...")
    # fetch_acts_from_csv(limit=3, delay=1)
    
    # Uncomment below to fetch all acts
    print("Fetching all acts...")
    fetch_acts_from_csv(delay=0.3)  # 0.3 seconds = 300ms delay
