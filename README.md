# Bangladesh Legal Acts Dataset

A comprehensive database of Bangladesh's legal framework, containing 1484+ acts scraped and processed from the official Bangladesh Laws portal, enhanced with historical government context, legal system context, and comprehensive metadata.

## 📊 Dataset Overview

- **Total Acts**: 1,484
- **Total Sections**: 35,633
- **Total Footnotes**: 14,523
- **Languages**: English, Bengali, Mixed
- **Format**: JSON with structured metadata
- **Historical Context**: Government periods from 1799-2025
- **Legal System Context**: Comprehensive legal framework information
- **Enhanced Features**: Token counts, year standardization, processing metadata
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

## 🔄 Data Processing Pipeline

The dataset has been enhanced through multiple processing stages:

1. **Initial Scraping**: Raw legal acts data collection
2. **Content Cleaning**: Text processing and structuring
3. **Detailed Fetching**: Section-by-section content extraction
4. **Data Combination**: Merging structured data
5. **Enhancement**: Token counting and language detection
6. **Government Context Integration**: Historical government information
7. **Legal System Context Addition**: Legal framework contextualization
8. **Year Translation**: Bengali to English numeral conversion
9. **Error Recovery**: Missing context restoration
10. **Token Count Correction**: Accurate tokenization across all fields
11. **Final Compilation**: Comprehensive dataset creation

## 🗂️ Repository Structure

```
├── Data/
│   ├── Contextualized_Bangladesh_Legal_Acts.json   # Final comprehensive dataset (NEW)
│   ├── acts/                                       # Individual enhanced act JSON files
│   ├── law.json                                    # Original combined dataset (Git LFS)
│   ├── processed_law.json                          # Enhanced dataset with tokens & language detection (Git LFS)
│   ├── processed_law_with_govt_context.json        # Dataset with government context (Git LFS)
│   ├── govt.json                                   # Government historical data
│   ├── bangladesh_legal_systems.json               # Legal system context data
│   └── meta.json                                   # Dataset metadata
├── Scripts/
│   ├── actScraper.py                               # Main scraping script
│   ├── actFetch.py                                 # Detailed content fetcher
│   ├── cleaner.py                                  # CSV data processing
│   ├── combine.py                                  # JSON combination script
│   ├── reducer.py                                  # Data enhancement script
│   ├── fusingGovtContext.py                        # Government context fusion script
│   ├── reduceAndFuseGovt.py                        # Combined reducer + government context
│   ├── legalContextAdd.py                          # Legal system context integration
│   ├── translateYear.py                            # Bengali year translation
│   ├── missingLegalContextAdd.py                   # Recovery script for missing contexts
│   ├── tokenCount.py                               # Token count correction script
│   └── finalCombine.py                             # Final dataset compilation
├── old_Code/                                       # Legacy processing scripts
├── filtered_act_list.csv                           # Processed act list (Git LFS)
└── README.md                                       # This file
```

## 🚀 Quick Start

### Prerequisites

```bash
pip install requests beautifulsoup4 pandas tqdm
```

### Complete Processing Pipeline

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

7. **Add legal system context**:
```bash
python legalContextAdd.py
```

8. **Fix token counts**:
```bash
python tokenCount.py
```

9. **Create final comprehensive dataset**:
```bash
python finalCombine.py
```

### Individual File Processing (Alternative)

For processing individual files with all enhancements:
```bash
python reduceAndFuseGovt.py
```

## 📋 Data Schema

### Final Dataset Structure (Contextualized_Bangladesh_Legal_Acts.json)

```json
{
  "dataset_info": {
    "name": "Contextualized Bangladesh Legal Acts Dataset",
    "description": "Comprehensive dataset with government and legal system context",
    "version": "1.0",
    "created_date": "2025-07-19T...",
    "total_acts": 1484,
    "data_sources": ["Bangladesh Government Legal Database", "Historical Government Context Data"],
    "processing_pipeline": ["Scraping", "Enhancement", "Context Integration", "Final Compilation"],
    "features": ["Full legal text", "Government context", "Legal system context", "Token counts"],
    "processing_stats": {
      "total_tokens": 2500000,
      "year_range": {"earliest": 1799, "latest": 2025},
      "unique_governments": 50,
      "unique_legal_systems": 12
    }
  },
  "acts": [...]
}
```

### Individual Act Structure (Enhanced)

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
    "government_name": "British Colonial Government",
    "govt_system": "Company Rule",
    "position_head_govt": "Governor-General of India",
    "head_govt_name": "Lord Canning (Charles Canning)",
    "head_govt_designation": "Governor-General of India",
    "how_got_power": "Company appointment",
    "period_years": "1856-1862",
    "years_in_power": 6,
    "context_added_at": "2025-07-19T..."
  },
  "legal_system_context": {
    "legal_framework": "British Colonial Legal System",
    "government_system": "Colonial Administration",
    "policing_system": "Imperial Police System",
    "land_relations": "Colonial Land Revenue System",
    "period": "1858-1947",
    "description": "British Crown rule with colonial administrative framework",
    "context_added_at": "2025-07-19T..."
  },
  "processing_info": {
    "government_context_source": "govt.json lookup",
    "legal_context_source": "bangladesh_legal_systems.json",
    "year_translation": "Original year retained",
    "token_count_method": "Comprehensive field tokenization",
    "last_updated": "2025-07-19T..."
  },
  "source_file": "act-print-11.json",
  "source_url": "http://bdlaws.minlaw.gov.bd/act-print-11.html",
  "fetch_timestamp": "2025-07-19 02:45:32"
}
```

## 🔍 Data Features

### Enhanced Processing
- **Missing Data Recovery**: Automatically fills gaps using CSV metadata
- **Token Counting**: Word-level tokenization for all text content including context fields
- **Language Detection**: Automatic detection of English/Bengali/Mixed content
- **Historical Government Context**: Matches each act with the government system and leadership at the time of enactment
- **Legal System Context**: Comprehensive legal framework information for each historical period
- **Year Standardization**: Bengali to English numeral translation
- **Error Recovery**: Automated recovery for files with missing contexts
- **Metadata Preservation**: Maintains source URLs and timestamps

### Quality Assurance
- **Error Handling**: Robust error logging and recovery mechanisms
- **Data Validation**: Automatic validation of combined datasets
- **Progress Tracking**: Real-time processing progress with tqdm
- **Statistics**: Comprehensive processing statistics and distribution analysis
- **Batch Processing**: Memory-efficient chunked processing
- **Comprehensive Logging**: Detailed logs for all processing stages

### Government Context Features
- **Historical Periods**: Covers government systems from 1799-2025
- **Leadership Information**: Includes head of government names and designations
- **Power Transitions**: Documents how each government came to power
- **System Classification**: Categorizes different government types (Company Rule, Colonial, Democratic, Military, etc.)
- **Temporal Mapping**: Precise year-based matching of acts to government periods

### Legal System Context Features
- **Legal Framework Classification**: Comprehensive categorization of legal systems
- **Government System Integration**: Links legal framework to government structure
- **Policing System Context**: Historical policing and law enforcement information
- **Land Relations Framework**: Land ownership and revenue system context
- **Temporal Coverage**: Complete coverage across all historical periods
- **Contextual Descriptions**: Detailed explanations of each legal system period

## 📈 Dataset Statistics

| Metric | Value |
|--------|--------|
| Total Legal Documents | 1,484 |
| Date Range | 1799 - 2025 |
| Languages | English, Bengali, Mixed |
| Total Sections | 35,633 |
| Total Footnotes | 14,523 |
| Government Periods Covered | 50+ historical periods |
| Legal System Periods | 12+ distinct legal frameworks |
| Government Systems | Company Rule, Colonial, Military, Democratic, etc. |
| Average Tokens per Act | ~2,884 |
| Total Dataset Tokens | 2,500,000+ |
| Processing Scripts | 10+ specialized processing tools |
| Processing Time | ~30 minutes (complete pipeline) |

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
- **Legal context integration**: Temporal period matching algorithms
- **Token counting**: Regex-based comprehensive tokenization
- **Error recovery**: Automated detection and correction of processing errors

### Processing Pipeline Features
- **Modular Design**: Each processing stage is independent and reusable
- **Progress Tracking**: Real-time progress bars and statistics
- **Comprehensive Logging**: File and console logging for all operations
- **Memory Management**: Garbage collection and chunked processing
- **Error Resilience**: Continues processing despite individual file failures
- **Statistics Generation**: Detailed processing and content statistics

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
