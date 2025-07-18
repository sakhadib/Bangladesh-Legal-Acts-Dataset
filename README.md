# Bangladesh Laws Database

A comprehensive database of Bangladesh's legal framework, containing 1484+ acts scraped and processed from the official Bangladesh Laws portal.

## 📊 Dataset Overview

- **Total Acts**: 1,484
- **Total Sections**: 35,633
- **Total Footnotes**: 14,523
- **Languages**: English, Bengali, Mixed
- **Format**: JSON with structured metadata
- **License**: CC-BY 4.0

## 🗂️ Repository Structure

```
├── Data/
│   ├── law.json              # Combined dataset (Git LFS)
│   ├── processed_law.json    # Enhanced dataset with tokens & language detection (Git LFS)
│   └── meta.json             # Dataset metadata
├── acts/                     # Individual act JSON files (Git LFS)
├── actScraper.py             # Main scraping script
├── actFetch.py               # Detailed content fetcher
├── cleaner.py                # CSV data processing
├── combine.py                # JSON combination script
├── reducer.py                # Data enhancement script
├── filtered_act_list.csv     # Processed act list (Git LFS)
└── README.md                 # This file
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
  "source_url": "http://bdlaws.minlaw.gov.bd/act-print-11.html",
  "fetch_timestamp": "2025-07-19 02:45:32"
}
```

## 🔍 Data Features

### Enhanced Processing
- **Missing Data Recovery**: Automatically fills gaps using CSV metadata
- **Token Counting**: Word-level tokenization for all text content
- **Language Detection**: Automatic detection of English/Bengali/Mixed content
- **Metadata Preservation**: Maintains source URLs and timestamps

### Quality Assurance
- **Error Handling**: Robust error logging and recovery
- **Data Validation**: Automatic validation of combined datasets
- **Progress Tracking**: Real-time processing progress
- **Statistics**: Comprehensive processing statistics

## 📈 Dataset Statistics

| Metric | Value |
|--------|--------|
| Total Legal Documents | 1,484 |
| Date Range | 1799 - 2025 |
| Languages | English, Bengali, Mixed |
| Total Sections | 35,633 |
| Total Footnotes | 14,523 |
| Average Tokens per Act | ~1,685 |
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

## 📚 Citation

If you use this dataset in your research, please cite:

```bibtex
@dataset{bangladesh_laws_2025,
  title={Bangladesh Laws Database},
  author={[Your Name]},
  year={2025},
  publisher={GitHub},
  url={https://github.com/[username]/bangladesh-laws},
  license={CC-BY-4.0}
}
```

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
