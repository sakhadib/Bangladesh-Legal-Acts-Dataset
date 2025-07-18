import requests
from bs4 import BeautifulSoup, Tag
import csv
import time
from urllib.parse import urljoin

# Define the website URL
website = 'http://bdlaws.minlaw.gov.bd/laws-of-bangladesh-chronological-index.html'  # Replace with actual URL

# Send an HTTP request to the website
try:
    response = requests.get(website)
    response.raise_for_status()  # Check for request errors
except requests.exceptions.RequestException as e:
    print(f"Error fetching the website: {e}")
    exit(1)

# Parse the page content using BeautifulSoup
soup = BeautifulSoup(response.text, 'html.parser')

# Open CSV file to write the data
with open('acts_data.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Act Title', 'Act No', 'Act Year', 'Act Link'])  # Write header
    
    # Find all rows in the table
    rows = soup.select('.datatable tbody tr')
    
    if not rows:
        print("No rows found. Please check the CSS selector '.datatable tbody tr'")
        print("Available tables:")
        tables = soup.find_all('table')
        for i, table in enumerate(tables):
            if isinstance(table, Tag):
                table_class = table.get('class')
                if table_class:
                    print(f"Table {i+1}: {table_class}")
                else:
                    print(f"Table {i+1}: No class")
    
    # Iterate over each row and extract data
    for row_index, row in enumerate(rows, 1):
        try:
            # Extract the columns in each row
            columns = row.find_all('td')
            
            if len(columns) > 2:
                # Find the anchor tag in the first column
                first_column = columns[0]
                if isinstance(first_column, Tag):
                    anchor_tag = first_column.find('a')
                    
                    # Check if anchor tag exists and is a Tag
                    if anchor_tag and isinstance(anchor_tag, Tag):
                        # Extract text content
                        act_title = anchor_tag.get_text(strip=True)
                        
                        # Extract href attribute
                        href = anchor_tag.get('href')
                        if href:
                            act_link = str(href)
                            
                            # Convert relative URL to absolute URL if needed
                            if act_link.startswith('/') or not act_link.startswith('http'):
                                act_link = urljoin(website, act_link)
                        else:
                            act_link = ''
                        
                        # Extract other column data
                        second_column = columns[1]
                        third_column = columns[2]
                        if isinstance(second_column, Tag) and isinstance(third_column, Tag):
                            act_no = second_column.get_text(strip=True)
                            act_year = third_column.get_text(strip=True)
                            
                            # Write the data into the CSV file
                            writer.writerow([act_title, act_no, act_year, act_link])
                            print(f"Row {row_index}: Scraped '{act_title}' ({act_year})")
                        else:
                            print(f"Row {row_index}: Skipping - invalid column types")
                    else:
                        print(f"Row {row_index}: Skipping - no anchor tag found in first column")
                else:
                    print(f"Row {row_index}: Skipping - first column is not a Tag")
            else:
                print(f"Row {row_index}: Skipping - insufficient columns ({len(columns)} found, need > 2)")
            
        except Exception as e:
            print(f"Row {row_index}: Error processing - {e}")
            continue

print("Scraping completed! Data saved to 'acts_data.csv'")
