# Copilot Instructions for Students Matching Repository

## Project Overview

This repository implements a student-to-voter matching system using fuzzy string matching and data processing techniques. The project matches student records with voter registration data based on names, birth years, and other demographic information.

## Technology Stack

- **Python 3.x**: Primary programming language
- **Data Processing Libraries**:
  - **Ibis**: SQL-like data manipulation with DuckDB backend
  - **Polars**: High-performance DataFrame library for data transformations
  - **Pandas**: Additional data manipulation and geocoding operations
  - **PyArrow**: Columnar data format and operations
- **Matching & Geocoding**:
  - **RapidFuzz**: Fast fuzzy string matching (Jaro-Winkler similarity)
  - **censusbatchgeocoder**: Geocoding student/voter addresses
- **Utilities**: tqdm (progress bars), os (file operations)

## Key Modules

### `helpers_matching.py`
Core data processing and matching logic. Key functions include:

- **String Normalization**: `clean_string()`, `normalize_string()` - Clean and normalize names by removing titles, special characters, and accents
- **Fuzzy Matching**: `fuzzy_score()`, `jw_sim()` - Jaro-Winkler similarity scoring (uses DuckDB's native implementation)
- **Data Processing**:
  - `process_data_students()` - Match students with voter records
  - `process_data_parents()` - Match parent records with voter data
- **Batch Processing**:
  - `run_matching_in_batches()` - Process large datasets in chunks
  - `merge_matching_batches()` - Combine batch results
- **Geocoding Pipeline**:
  - `make_geocode_files()` - Prepare addresses for geocoding
  - `run_geocoding_queries_by_batches()` - Geocode addresses using Census API
  - `merge_geo_to_main_data()` - Join geocoded data back to main dataset
- **Output Generation**: `make_partitions_by_school_year()` - Create per-school, per-cohort CSV files

### `matching_students.py`
Main script for orchestrating the matching workflow. Contains commented examples of running different matching operations for 2019 and 2024 cohorts.

## Code Style Guidelines

1. **String Operations**: Always use Ibis/Polars expressions rather than applying Python string functions row-by-row
2. **Null Handling**: Use `.fill_null()` or `.isnull()` checks consistently
3. **Column Naming Convention**:
   - `cf_` prefix for student/cohort fields
   - `vf_` prefix for voter file fields
   - `geo_` prefix for geocoded fields
4. **Data Cleaning**: Apply `clean_string()` to all name fields before matching
5. **Fuzzy Matching Thresholds**: Default fuzzy score threshold is 90 for matching predicates
6. **Birth Year Tolerance**: Default tolerance is 5-7 years when matching by age

## Matching Logic

The matching algorithm uses multiple predicates to reduce false positives:

1. **First/Last Character Matching**: Check first and last characters of first/last names match
2. **Fuzzy Similarity**: Jaro-Winkler similarity > 90 for both first and last names
3. **Birth Year Range**: Students matched within age tolerance (typically cohort year - 21 ± tolerance)
4. **Middle Initial Flag**: Optional additional verification using middle initials

## Data Flow

```
Input Data (Parquet files)
    ↓
1. Clean/Normalize Names
    ↓
2. Fuzzy Matching (students/parents ↔ voters)
    ↓
3. Batch Processing (handle large datasets)
    ↓
4. Merge Batches
    ↓
5. Geocode Addresses (optional)
    ↓
6. Partition by School/Cohort
    ↓
Output CSV files
```

## Working with This Codebase

### When Making Changes:

1. **Performance**: Use Ibis/Polars lazy evaluation when possible (`.scan_parquet()`, collect with `streaming=True`)
2. **Memory Management**: Process large files in batches to avoid memory issues
3. **Fuzzy Matching**: Test threshold changes carefully - too low increases false positives, too high increases false negatives
4. **Geocoding**: Census batch geocoding has rate limits; include retry logic and delays
5. **File Operations**: Always use absolute paths or properly join paths with `os.path.join()`

### Testing:

- No formal test suite currently exists
- Test changes by running on a small subset of data first
- Verify output by checking:
  - Record counts match expectations
  - Fuzzy scores are within acceptable ranges
  - No duplicate matches per student
  - Geocoding success rates are reasonable

### Dependencies:

Key dependencies (install via pip):
```bash
pip install ibis-framework[duckdb] polars pandas pyarrow rapidfuzz censusbatchgeocoder tqdm
```

## Common Operations

### Running a Full Matching Pipeline:
```python
# 1. Run matching in batches
run_matching_in_batches('output_dir/', 'input_dir/', 'base_file.parquet', 'students', 7)

# 2. Merge results
merge_matching_batches('output_dir/', 'final_output.parquet')

# 3. Add geocoding (optional)
add_geocoding_for_students(2024)

# 4. Partition by school/year
make_partitions_by_school_year('final_output.parquet', 'partitioned_output/')
```

### Adjusting Fuzzy Match Threshold:
Modify the predicates in `process_data_students()` or `process_data_parents()` functions - look for lines like:
```python
fuzzy_score(table1.merge_var, table2.merge_var) > 90
```

## Important Notes

- **Data Privacy**: This codebase handles sensitive student and voter information - maintain appropriate data security practices
- **DuckDB Native Functions**: The `jw_sim` UDF is a placeholder that calls DuckDB's native Jaro-Winkler implementation
- **String Cleaning**: The `clean_string()` function removes a comprehensive list of titles and suffixes - review before modifying
- **Lazy Evaluation**: Most operations use lazy evaluation to handle datasets larger than memory
- **Parquet Format**: All intermediate and final outputs use Parquet for efficient storage and processing

## File Structure

```
.
├── helpers_matching.py          # Core matching and processing functions
├── matching_students.py         # Main orchestration script
├── Full_students_2019_10_14_2025/   # Input data folder (2019 cohort)
├── Full_students_2024_10_14_2025/   # Input data folder (2024 cohort)
└── .gitignore                   # Excludes *.parquet files
```

## Getting Help

When asking Copilot for help with this repository:
- Specify whether you're working with student or parent matching
- Mention the cohort year if relevant (2019, 2024, etc.)
- Include context about the data processing stage (matching, geocoding, partitioning)
- Reference specific function names from `helpers_matching.py` when possible
