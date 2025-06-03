# RavenPack News Processor

## Description

The `RavenPackNewsProcessor` is a Python class designed for processing RavenPack news event data retrieved from WRDS. It fetches raw event data, preprocesses it (including time zone conversion, deduplication, and headline cleaning), merges company identifiers, and filters events based on user-provided keywords (supporting both unigrams and bigrams). Processed data can be exported as Parquet files for further analysis.

---

## Features

- Bulk download of RavenPack event data by year from WRDS 
- Supports both unigram and bigram keyword filtering  
- Automatic timezone conversion from UTC to US/Eastern  
- Deduplication of events using DuckDB  
- Efficient identifier merging using company link tables  
- Outputs to `.parquet` files with compression for easy downstream processing

---

## Test Environment

- Python == 3.12.7
- pandas == 2.2.3
- polars == 1.27.1
- duckdb == 1.1.3
- tqdm == 4.66.5
- wrds == 3.2.0
- pyarrow == 20.0.0

Install them via pip:

```bash
pip install pandas polars duckdb tqdm wrds pyarrow
```

---

## Usage

```python
import wrds
from ravenpack_processor import RavenPackNewsProcessor

# Connect to WRDS
conn = wrds.Connection()

# Initialize processor
processor = RavenPackNewsProcessor(
    keywords_path='./keywords.txt',  # Path to keyword file
    start_year=2015,
    end_year=2016,
    output_dir='./output',           # Optional
    wrds_conn=conn
)

# Process RavenPack data
res = processor.process_ravenpacks()
```

---

## Keyword Format

The `keywords.txt` file should include one keyword per line:

```nginx
earnings
interest rate
merger
```

- Unigrams: `earnings`, `merger`
- Bigrams: `interest rate`

Both unigrams and bigrams are supported and handled internally with regular expressions for token-boundary matching.

---

## Notes

- Both unigram and bigram keywords are supported. Bigrams are automatically transformed into regular expressions that match sequences of words, allowing for flexible text matching in headlines.
- Users **must have private WRDS credentials** to access `RavenPack` and company link tables from WRDS.
- If the keyword list is too long (thousands of entries), regular expression matching may introduce performance bottlenecks due to very large pattern compilation. Consider batching keywords or using a more efficient text-matching approach if needed.

---

For any questions or issues, please contact the developer or submit an issue.
