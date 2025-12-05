# Student-Voter Matching System

This repository contains a data matching system designed to match student records with voter registration data using fuzzy string matching and demographic filters. The system processes large datasets in batches and supports geocoding of matched addresses.

## Overview

The codebase implements a sophisticated matching algorithm that:
- Matches students and parents to voter registration records
- Uses fuzzy string matching (Jaro-Winkler similarity) for name matching
- Applies demographic filters (birth year tolerance) to improve accuracy
- Processes data in batches for memory efficiency
- Supports optional geocoding of matched addresses
- Partitions results by school and cohort year for easy analysis

## Project Structure

```
.
├── matching_students.py      # Main script with matching workflow examples
├── helpers_matching.py        # Core matching functions and utilities
├── requirements.txt           # Python package dependencies
├── Full_students_2019_10_14_2025/  # Input directory for 2019 student data (empty initially)
├── Full_students_2024_10_14_2025/  # Input directory for 2024 student data (empty initially)
└── README.md                  # This file
```

### Generated Output Directories

The scripts will create various output directories during execution:
- `2019_reduced/`, `2024_reduced/` - Reduced/processed matching results
- `matches_students_YYYY_10_15_2025_by_cohort_year/` - Results partitioned by cohort
- `geocode_students_parts_YYYY/` - Geocoding partition files
- Various `.parquet` files with intermediate and final results

## Prerequisites

- Python 3.8 or higher
- pip (Python package manager)
- Sufficient disk space for processing large parquet files

## Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd Students_matching_Menaka_Hampole
```

### 2. Set Up Virtual Environment (Recommended)

It's highly recommended to use a virtual environment to avoid conflicts with system packages:

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On Linux/Mac:
source venv/bin/activate
# On Windows:
venv\Scripts\activate
```

### 3. Install Dependencies

Install all required packages using the requirements.txt file:

```bash
pip install -r requirements.txt
```

This will install:
- **ibis-framework[duckdb]** - SQL-like data processing with DuckDB backend
- **pandas** - Data manipulation library
- **polars** - Fast DataFrame library for large datasets
- **pyarrow** - Columnar data format for efficient I/O
- **rapidfuzz** - Fast fuzzy string matching
- **censusbatchgeocoder** - Geocoding using US Census API
- **tqdm** - Progress bar utility

### 4. Verify Installation

```bash
python -c "import ibis, pandas, polars, pyarrow, rapidfuzz, censusbatchgeocoder, tqdm; print('All packages installed successfully!')"
```

## Data Preparation

### Input Data Format

The system expects the following input files:

1. **Student/Parent Data** (Parquet format):
   - Required columns for students: `name`, `firstname`, `middlename`, `lastname`, `cohort`, `school`
   - Required columns for parents: `firstname`, `middlename`, `lastname`, `relative_name`, `birth_cohort`

2. **Voter Data** (Parquet files in batch directories):
   - Required columns: `Voters_FirstName`, `Voters_LastName`, `Voters_MiddleName`, `Voters_BirthDate`
   - Address columns: `Voters_addresses_addressline`, `Voters_addresses_city`, `Voters_addresses_state`, `Voters_addresses_zip`
   - Additional voter info columns (see code for full list)

3. **Directory Structure**:
   Place voter data parquet files in the appropriate year directory:
   - `Full_students_2019_10_14_2025/` for 2019 matches
   - `Full_students_2024_10_14_2025/` for 2024 matches

## Usage

### Basic Workflow

The main script (`matching_students.py`) contains commented examples of the typical workflow. Here's how to use it:

#### 1. Configure Your Matching Run

Edit `matching_students.py` and uncomment the desired operations:

```python
import ibis
from helpers_matching import *

# Example: Run matching for 2024 students
run_matching_in_batches(
    'Full_students_2024_10_14_2025/',  # Output directory
    '2024_reduced/',                     # Input voter data directory
    'total_cohorts_09_22_2025.parquet', # Student data file
    'students',                          # Match type: 'students' or 'parents'
    7                                    # Birth year tolerance
)
```

#### 2. Run the Script

```bash
python matching_students.py
```

### Key Functions

#### `run_matching_in_batches(res_dir, folder, base_file, type, birth_year_tol)`

Runs the matching process in batches for memory efficiency.

**Parameters:**
- `res_dir` (str): Output directory for matched results
- `folder` (str): Directory containing voter data parquet files
- `base_file` (str): Path to student/parent data file
- `type` (str): Either `'students'` or `'parents'`
- `birth_year_tol` (int): Birth year tolerance for matching (default: 5)

**Example:**
```python
run_matching_in_batches(
    'Full_students_2024_10_14_2025/',
    '2024_reduced/',
    'total_cohorts_09_22_2025.parquet',
    'students',
    7
)
```

#### `merge_matching_batches(folder, output_name)`

Merges all batch results into a single output file with proper formatting.

**Parameters:**
- `folder` (str): Directory containing batch result parquet files
- `output_name` (str): Output filename for merged results

**Example:**
```python
merge_matching_batches(
    'Full_students_2019_10_14_2025/',
    'matches_students_2019_formatted_10_14_2025.parquet'
)
```

#### `make_partitions_by_school_year(input_filename, result_dir)`

Creates CSV files partitioned by school and cohort year.

**Parameters:**
- `input_filename` (str): Input parquet file with matched data
- `result_dir` (str): Output directory for partitioned CSV files

**Example:**
```python
make_partitions_by_school_year(
    'matches_students_2019_formatted_10_14_2025.parquet',
    'matches_students_2019_10_15_2025_by_cohort_year/'
)
```

#### `add_geocoding_for_students(year)`

Adds geocoding information to matched student records using US Census API.

**Parameters:**
- `year` (int): Year identifier (e.g., 2019, 2024)

**Example:**
```python
add_geocoding_for_students(2024)
```

**Note:** Geocoding requires internet connectivity and may take significant time for large datasets.

### Complete Workflow Example

```python
import ibis
from helpers_matching import *

# 1. Run matching in batches
print('Running matching for 2024 students...')
run_matching_in_batches(
    'Full_students_2024_10_14_2025/',
    '2024_reduced/',
    'total_cohorts_09_22_2025.parquet',
    'students',
    7
)

# 2. Merge batch results
print('Merging batches...')
merge_matching_batches(
    'Full_students_2024_10_14_2025/',
    'matches_students_2024_formatted_10_14_2025.parquet'
)

# 3. (Optional) Add geocoding
print('Adding geocoding...')
add_geocoding_for_students(2024)

# 4. Create partitions by school and cohort
print('Creating partitions...')
make_partitions_by_school_year(
    'matches_students_2024_formatted_10_14_2025.parquet',
    'matches_students_2024_10_15_2025_by_cohort_year/'
)

print('Done!')
```

## Matching Algorithm

### For Students

The matching algorithm uses the following criteria:

1. **Name Matching:**
   - First character of first name must match
   - Last character of first name must match
   - First character of last name must match
   - Fuzzy score (Jaro-Winkler) > 90 for both first and last names

2. **Age Filter:**
   - Voter birth year must be within: `(cohort - 21 - tolerance, cohort - 21 + tolerance)`
   - Default tolerance: 5-7 years

3. **Name Normalization:**
   - Removes titles (Jr, Sr, Dr, etc.)
   - Removes special characters
   - Converts to lowercase
   - Normalizes unicode characters

### For Parents

Similar to student matching but uses:
- Parent birth cohort instead of student cohort
- Different age calculation logic
- Matches on relative name field

## Output Format

### Matched Records

The output parquet files contain:
- `cf_*` prefix: Student/parent data columns (from College File)
- `vf_*` prefix: Voter registration data columns (from Voter File)
- `fuzzy_score_first`: Fuzzy match score for first names (0-100)
- `fuzzy_score_last`: Fuzzy match score for last names (0-100)
- `middle_initial_flag`: 1 if middle initials match, 0 otherwise
- `id_number`: Unique identifier for each match
- `vf_residence_address`: Concatenated residence address
- `vf_mailing_address`: Concatenated mailing address

### Partitioned CSV Files

When using `make_partitions_by_school_year()`, output files are named:
```
{school_name}_{cohort_year}.csv
```

## Performance Considerations

- **Memory:** The system uses lazy evaluation (Polars/Ibis) to handle large datasets
- **Batching:** Voter data should be split into manageable batch files
- **Geocoding:** Can be slow for large datasets; consider running separately
- **Disk Space:** Ensure sufficient space for intermediate parquet files

## Troubleshooting

### Import Errors

If you get import errors, ensure all packages are installed:
```bash
pip install -r requirements.txt --upgrade
```

### Memory Issues

If you encounter memory issues:
- Reduce batch size in voter data partitioning
- Use streaming mode: `merge_matching_batches(..., streaming_mode=True)`
- Process years separately

### Geocoding Failures

The geocoding function includes retry logic (10 attempts with 20-second delays). If it continues to fail:
- Check internet connectivity
- Verify US Census Geocoding API is accessible
- Consider reducing batch sizes in `gen_geocode_partitions()`

### DuckDB Errors

If you encounter DuckDB-related errors:
```bash
pip install --upgrade ibis-framework[duckdb] duckdb
```

## Data Privacy and Security

⚠️ **Important:** This system processes personally identifiable information (PII). Please ensure:
- Compliance with applicable data protection regulations
- Secure storage of input and output files
- Proper access controls
- Data retention policies are followed

## Contributing

When contributing to this repository:
1. Maintain the existing code structure
2. Document any new functions thoroughly
3. Update this README with new features
4. Test with sample data before processing full datasets

## License

[Add license information here]

## Contact

[Add contact information here]

## Acknowledgments

This matching system was developed for educational data analysis and voter registration matching research.
