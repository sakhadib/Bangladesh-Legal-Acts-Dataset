import pandas as pd
import re

# Read the CSV file
df = pd.read_csv('acts_data.csv')

print(f"Original dataset shape: {df.shape}")
print(f"Columns: {list(df.columns)}")

# Task 1: Add 'is_repealed' column based on '[Repealed]' or '[রহিত]' keyword in title
# Check for both English '[Repealed]' and Bengali '[রহিত]' keywords
df['is_repealed'] = df['Act Title'].str.contains(r'\[Repealed\]|\[রহিত\]', case=False, na=False)

# Task 2: Add 'full_text' column by modifying the link
def create_full_text_link(link):
    """
    Convert regular link to full text link
    Example: http://bdlaws.minlaw.gov.bd/act-599.html 
    -> http://bdlaws.minlaw.gov.bd/act-print-599.html
    """
    if pd.isna(link) or not isinstance(link, str):
        return ''
    
    # Use regex to replace 'act-' with 'act-print-'
    full_text_link = re.sub(r'act-(\d+)\.html', r'act-print-\1.html', link)
    return full_text_link

# Apply the function to create full_text column
df['full_text'] = df['Act Link'].apply(create_full_text_link)

# Reorder columns to match the requested structure:
# Act Title, Act No, Act Year, Act Link, full_text, is_repealed
column_order = ['Act Title', 'Act No', 'Act Year', 'Act Link', 'full_text', 'is_repealed']
df = df[column_order]

# Display some statistics
print(f"\nDataset statistics:")
print(f"Total acts: {len(df)}")
print(f"Repealed acts: {df['is_repealed'].sum()}")
print(f"Active acts: {(~df['is_repealed']).sum()}")

# Show breakdown of repealed acts by language
english_repealed = df['Act Title'].str.contains(r'\[Repealed\]', case=False, na=False).sum()
bengali_repealed = df['Act Title'].str.contains(r'\[রহিত\]', case=False, na=False).sum()
print(f"English repealed acts [Repealed]: {english_repealed}")
print(f"Bengali repealed acts [রহিত]: {bengali_repealed}")

# Display first few rows to verify
print(f"\nFirst 5 rows:")
print(df.head())

# Display some examples of repealed acts
repealed_acts = df[df['is_repealed'] == True]
if len(repealed_acts) > 0:
    print(f"\nExample repealed acts:")
    print(repealed_acts[['Act Title', 'Act Year', 'is_repealed']].head())

# Display some examples of full text links
print(f"\nExample full text link conversions:")
print(df[['Act Link', 'full_text']].head())

# Save the processed data
output_file = 'filtered_act_list.csv'
df.to_csv(output_file, index=False, encoding='utf-8')

print(f"\nProcessed data saved to: {output_file}")
print(f"Final dataset shape: {df.shape}")
