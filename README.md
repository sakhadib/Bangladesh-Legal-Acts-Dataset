# Bangladesh Legal Acts Dataset

A comprehensive database of Bangladesh's legal framework, containing 1484+ acts scraped and processed from the official Bangladesh Laws portal, enhanced with historical government context.

## 📊 Dataset Overview

- **Total Acts**: 1,484
- **Total Sections**: 35,633
- **Total Footnotes**: 14,523
- **Languages**: English, Bengali, Mixed
- **Format**: JSON with structured metadata
- **Historical Context**: Government periods from 1799-2025
- **License**: CC-BY 4.0

[![View Dataset on Kaggle](https://img.shields.io/badge/Kaggle-View_Dataset-blue?logo=kaggle)](https://www.kaggle.com/dsv/12511542)

## 📚 Citation

If you use this dataset in your research or project, please cite it as:

```bibtex
@misc{adib_sakhawat_2025,
  title     = {Bangladesh Legal Acts Dataset},
  url       = {https://www.kaggle.com/dsv/12511542},
  DOI       = {10.34740/KAGGLE/DSV/12511542},
  publisher = {Kaggle},
  author    = {Adib Sakhawat},
  year      = {2025}
}
```


## 🗂️ Repository Structure

```
├── Data/
│   ├── law.json                                    # Combined dataset (Git LFS)
│   ├── processed_law.json                          # Enhanced dataset with tokens & language detection (Git LFS)
│   ├── processed_law_with_govt_context.json        # Final dataset with government context (Git LFS)
│   ├── govt.json                                   # Government historical data
│   └── meta.json                                   # Dataset metadata
├── acts/                                           # Individual act JSON files (Git LFS)
├── actScraper.py                                   # Main scraping script
├── actFetch.py                                     # Detailed content fetcher
├── cleaner.py                                      # CSV data processing
├── combine.py                                      # JSON combination script
├── reducer.py                                      # Data enhancement script
├── fusingGovtContext.py                            # Government context fusion script
├── filtered_act_list.csv                           # Processed act list (Git LFS)
└── README.md                                       # This file
```

## 🚀 Quick Start

### Prerequisites

```bash
pip install requests beautifulsoup4 pandas
```

### Usage

1. **Scrape act list**:
```bash
python actScraper.py
```

2. **Process the scraped data**:
```bash
python cleaner.py
```

3. **Fetch detailed content**:
```bash
python actFetch.py
```

4. **Combine into single dataset**:
```bash
python combine.py
```

5. **Enhance with tokens and language detection**:
```bash
python reducer.py
```

6. **Add historical government context**:
```bash
python fusingGovtContext.py
```

## 📋 Data Schema

### Main Dataset Structure

```json
{
  "metadata": {
    "total_acts": 1484,
    "successful_files": 1484,
    "failed_files": 0,
    "total_sections": 35633,
    "total_footnotes": 14523,
    "language_distribution": {
      "english": 1200,
      "bengali": 250,
      "mixed": 34
    },
    "token_statistics": {
      "total_tokens": 2500000,
      "average_tokens_per_act": 1685
    }
  },
  "acts": [...]
}
```

### Individual Act Structure

```json
{
  "act_title": "The Penal Code, 1860",
  "act_no": "XLV",
  "act_year": "1860",
  "publication_date": "6th October, 1860",
  "language": "english",
  "token_count": 15420,
  "is_repealed": false,
  "sections": [
    {
      "section_title": "Chapter I - Introduction",
      "section_content": "This Act may be called the Penal Code..."
    }
  ],
  "footnotes": [
    {
      "footnote_text": "Extended to Bangladesh by..."
    }
  ],
  "csv_metadata": {
    "act_title_from_csv": "The Penal Code, 1860",
    "is_repealed": false
  },
  "government_context": {
    "govt_system": "Company Rule",
    "position_head_govt": "Governor-General of India",
    "head_govt_name": "Lord Canning (Charles Canning)",
    "head_govt_designation": "Governor-General of India",
    "how_got_power": "Company appointment",
    "period_years": "1856-1862",
    "years_in_power": 6
  },
  "source_url": "http://bdlaws.minlaw.gov.bd/act-print-11.html",
  "fetch_timestamp": "2025-07-19 02:45:32"
}
```

## 🔍 Data Features

### Enhanced Processing
- **Missing Data Recovery**: Automatically fills gaps using CSV metadata
- **Token Counting**: Word-level tokenization for all text content
- **Language Detection**: Automatic detection of English/Bengali/Mixed content
- **Historical Government Context**: Matches each act with the government system and leadership at the time of enactment
- **Metadata Preservation**: Maintains source URLs and timestamps

### Quality Assurance
- **Error Handling**: Robust error logging and recovery
- **Data Validation**: Automatic validation of combined datasets
- **Progress Tracking**: Real-time processing progress
- **Statistics**: Comprehensive processing statistics

### Government Context Features
- **Historical Periods**: Covers government systems from 1799-2025
- **Leadership Information**: Includes head of government names and designations
- **Power Transitions**: Documents how each government came to power
- **System Classification**: Categorizes different government types (Company Rule, Colonial, Democratic, Military, etc.)
- **Temporal Mapping**: Precise year-based matching of acts to government periods

## 📈 Dataset Statistics

| Metric | Value |
|--------|--------|
| Total Legal Documents | 1,484 |
| Date Range | 1799 - 2025 |
| Languages | English, Bengali, Mixed |
| Total Sections | 35,633 |
| Total Footnotes | 14,523 |
| Government Periods Covered | 50+ historical periods |
| Government Systems | Company Rule, Colonial, Military, Democratic, etc. |
| Average Tokens per Act | ~2,884 |
| Processing Time | ~19 minutes |

## 🛠️ Technical Details

### Scraping Process
- **Respectful scraping**: 300ms delays between requests
- **Error resilience**: Continues processing despite individual failures
- **Memory optimization**: Batch processing for large datasets
- **Rate limiting**: Prevents server overload

### Data Processing
- **Batch optimization**: Processes 50-100 items per batch
- **Memory efficiency**: Streaming JSON writing for large files
- **Unicode support**: Full Bengali/English text support
- **Incremental processing**: Skip already processed files
- **Chunked processing**: Optimized memory usage for large datasets
- **Government context fusion**: Efficient lookup-based government matching

## 🏛️ Historical Government Context

The dataset includes comprehensive historical government context for each legal act, covering:

### Government Systems Covered
- **Company Rule (1799-1858)**: East India Company administration
- **British Colonial Rule (1858-1947)**: Direct British Crown rule
- **Pakistan Period (1947-1971)**: Dominion and Republic of Pakistan
- **Bangladesh Independence (1971-present)**: Various democratic and military governments

### Context Information
- Government system type and duration
- Head of government name and official designation
- Method of acquiring power (election, appointment, coup, etc.)
- Exact period years for historical accuracy

This historical context enables researchers to:
- Analyze legal evolution across different political systems
- Study the impact of government changes on legislation
- Understand the political circumstances surrounding specific laws
- Conduct temporal analysis of legal frameworks

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## ⚖️ Legal Notice

This dataset contains legal documents from the Bangladesh government. While the data is publicly available, please:

- Verify accuracy for legal purposes
- Check for updates on the official portal
- Respect the original source attribution

## 📄 License

This work is licensed under [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/).

You are free to:
- Share and redistribute
- Adapt and transform
- Use commercially

With attribution to this repository.

## 🔗 Related Links

- [Bangladesh Laws Portal](http://bdlaws.minlaw.gov.bd/)
- [Ministry of Law, Justice and Parliamentary Affairs](http://www.minlaw.gov.bd/)
- [Creative Commons CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/)

## 📞 Contact

For questions about this dataset, please open an issue in this repository.

---

**Disclaimer**: This is an unofficial compilation. For legal purposes, always refer to the official Bangladesh Laws portal.
