# AGENTS.md

## AI Agent Development Guide

This document provides guidance for AI agents working with this codebase. It outlines the architecture, coding patterns, and best practices specific to this student-voter matching system.

## Codebase Overview

### Purpose
This system performs fuzzy matching between student/parent records and voter registration data, using sophisticated string matching algorithms and demographic filters.

### Key Technologies
- **Ibis Framework**: SQL-like operations with lazy evaluation and DuckDB backend
- **Polars**: High-performance DataFrame library for data processing
- **PyArrow**: Efficient columnar data format for I/O operations
- **DuckDB**: Embedded analytical database for complex queries
- **RapidFuzz**: Fast Jaro-Winkler similarity calculations

## Architecture

### Module Structure

#### `helpers_matching.py`
Contains all core functionality:
- Data processing functions (`process_data_students`, `process_data_parents`)
- String normalization and cleaning (`clean_string`, `normalize_string`)
- Fuzzy matching logic (`fuzzy_score`, `jw_sim`)
- Batch processing utilities
- Geocoding integration
- File I/O helpers

#### `matching_students.py`
Main entry point with workflow orchestration:
- Example usage patterns
- Commented workflow steps
- Configuration for different matching scenarios

### Data Flow

```
Input Data (Parquet)
    ↓
Normalization & Cleaning
    ↓
Fuzzy Matching (Batched)
    ↓
Merge & Format Results
    ↓
(Optional) Geocoding
    ↓
Partition by School/Cohort
    ↓
Output (Parquet/CSV)
```

## Design Patterns

### 1. Lazy Evaluation Pattern
The codebase extensively uses lazy evaluation for memory efficiency:

```python
# Polars lazy frames
df = pl.scan_parquet("file.parquet")  # No data loaded yet
df = df.filter(...)  # Query building
df = df.select(...)  # More operations
result = df.collect()  # Execution happens here
```

**For AI Agents:** When modifying data processing logic, maintain lazy evaluation chains. Avoid calling `.collect()` until necessary.

### 2. Batch Processing Pattern
Large datasets are processed in chunks:

```python
def run_matching_in_batches(res_dir, folder, base_file, type, birth_year_tol):
    files = find_parquet_files(folder)
    for file in tqdm(files):
        df = process_data_students(base_file, file, birth_year_tol)
        df.to_parquet(output_path)
```

**For AI Agents:** When adding new processing steps, ensure they work within the batch processing framework.

### 3. Column Prefixing Convention
Output columns use prefixes to identify their source:
- `cf_*` - College File (student/parent data)
- `vf_*` - Voter File (voter registration data)
- `geo_*` - Geocoding results

**For AI Agents:** Maintain this naming convention when adding new columns or features.

### 4. Ibis UDF Pattern
Custom functions are registered as Ibis UDFs for execution within DuckDB:

```python
@ibis.udf.scalar.pyarrow
def normalize_string(s: str) -> str:
    # Implementation
    
@ibis.udf.scalar.builtin(name="jaro_winkler_similarity")
def jw_sim(a: str, b: str) -> float:
    # DuckDB built-in function wrapper
```

**For AI Agents:** Use appropriate UDF decorators based on execution context (PyArrow for Python logic, builtin for DuckDB native functions).

## Critical Code Sections

### 1. String Normalization (`clean_string`)
**Location:** `helpers_matching.py:25-28`

This function is critical for matching quality. It:
- Normalizes unicode characters
- Removes titles and suffixes
- Strips special characters
- Converts to lowercase

**Caution:** Changes to this function affect all matching results. Test thoroughly with diverse name patterns.

### 2. Matching Predicates
**Location:** `helpers_matching.py:90-105` (students), `157-168` (parents)

These define the core matching logic:
- Character position matching (optimization)
- Fuzzy score thresholds
- Age/birth year filtering

**Caution:** Modifying predicates significantly impacts:
- Match recall and precision
- Query performance
- Result volume

### 3. Geocoding Pipeline
**Location:** `helpers_matching.py:436-459`

Multi-step process with external API calls:
- Rate limiting considerations
- Retry logic for failures
- Batch size management

**Caution:** Geocoding is expensive and slow. Test with small samples first.

## Common Tasks for AI Agents

### Task 1: Adjusting Fuzzy Match Threshold

**Current threshold:** 90 (line 96, 97 in `helpers_matching.py`)

To adjust:
```python
# In process_data_students predicates
fuzzy_score(students_table.merge_var, voters_table.merge_var) > NEW_THRESHOLD,
fuzzy_score(students_table.merge_var2, voters_table.merge_var2) > NEW_THRESHOLD,
```

**Impact:** 
- Higher threshold (>90): Fewer but more accurate matches
- Lower threshold (<90): More matches but potentially false positives

### Task 2: Adding New Data Fields

**Steps:**
1. Ensure field exists in input data
2. Add to appropriate table (students or voters)
3. Include in final selection (line 124-135)
4. Update output column list

**Example:**
```python
# In process_data_students
final_data = joined_data.mutate(
    # ... existing fields ...
    new_field=some_transformation(joined_data["source_column"])
)
```

### Task 3: Modifying Birth Year Tolerance

**Current default:** 5-7 years

**Location:** Function parameter in `run_matching_in_batches`

**Formula:** Voter birth year must be in range:
```
(cohort - 21 - tolerance, cohort - 21 + tolerance)
```

**Example:**
```python
# More strict matching (±3 years)
run_matching_in_batches(..., birth_year_tol=3)

# More lenient matching (±10 years)
run_matching_in_batches(..., birth_year_tol=10)
```

### Task 4: Optimizing Performance

**Key areas:**
1. **Predicate ordering:** Most selective predicates first
2. **Column selection:** Select only needed columns early
3. **Batch size:** Adjust based on available memory
4. **Index usage:** Ensure DuckDB can use indexes effectively

**Example optimization:**
```python
# Before
df = table.filter(expensive_condition).filter(cheap_condition)

# After
df = table.filter(cheap_condition).filter(expensive_condition)
```

## Testing Strategies

### Unit Testing
When adding new functions:
1. Test with edge cases (NULL values, empty strings, unicode)
2. Verify output schema matches expectations
3. Check memory usage with large inputs

### Integration Testing
For workflow changes:
1. Use small sample datasets
2. Verify end-to-end pipeline
3. Compare output statistics with baseline
4. Check file sizes are reasonable

### Sample Test Data
Create minimal test datasets:
```python
# Minimal student record
students = pl.DataFrame({
    'name': ['John Doe'],
    'firstname': ['John'],
    'lastname': ['Doe'],
    'middlename': ['M'],
    'cohort': [2020],
    'school': ['Test School']
})
```

## Error Handling Patterns

### Geocoding Errors
The code includes retry logic:
```python
retries = 10
while not success and attempt < retries:
    try:
        # geocoding attempt
    except Exception as e:
        attempt += 1
        pause(20)
```

**For AI Agents:** Follow this pattern for external API calls.

### File Processing Errors
```python
try:
    df = pl.read_parquet(file)
except Exception as e:
    print(f"Error reading {file}: {e}")
    continue
```

**For AI Agents:** Implement graceful degradation for batch processing.

## Performance Benchmarks

### Expected Processing Times
- **Matching:** ~5-10 minutes per million voter records
- **Merging batches:** ~2-5 minutes for 100 batch files
- **Geocoding:** ~1 hour per 10,000 unique addresses
- **Partitioning:** ~1-2 minutes per output year

### Memory Requirements
- **Typical:** 4-8 GB RAM for standard datasets
- **Large scale:** 16-32 GB RAM recommended
- **Streaming mode:** Can handle datasets larger than memory

## Dependencies and Versions

### Critical Version Requirements
- **Ibis:** Must support DuckDB backend (>=9.0.0)
- **Polars:** Must support lazy evaluation (>=0.19.0)
- **DuckDB:** Automatically installed with ibis[duckdb]

### Breaking Changes to Watch
- Ibis API changes (UDF decorators, backend initialization)
- Polars expression API updates
- DuckDB function signature changes

## Code Style Guidelines

### Naming Conventions
- Functions: `snake_case`
- Variables: `snake_case`
- Constants: `UPPER_SNAKE_CASE`
- Columns: Prefixed as described above

### Documentation
- Document all public functions with docstrings
- Include parameter types and descriptions
- Provide usage examples for complex functions

### Comments
- Use comments to explain "why," not "what"
- Document non-obvious optimizations
- Flag areas needing future improvement

## Common Pitfalls

### 1. Breaking Lazy Evaluation
❌ **Don't:**
```python
df = pl.scan_parquet("file.parquet").collect()
df = df.filter(...)  # Too late, already materialized
```

✅ **Do:**
```python
df = pl.scan_parquet("file.parquet")
df = df.filter(...)
result = df.collect()  # Collect only when needed
```

### 2. Memory Leaks in Loops
❌ **Don't:**
```python
results = []
for file in files:
    results.append(pl.read_parquet(file))  # Accumulates in memory
```

✅ **Do:**
```python
for file in files:
    df = pl.read_parquet(file)
    process_and_save(df)  # Process and release
```

### 3. Incorrect Column References After Join
❌ **Don't:**
```python
joined = left.join(right, on='id')
joined.select('name')  # Ambiguous if both have 'name'
```

✅ **Do:**
```python
joined = left.join(right, on='id')
joined.select(left['name'])  # Explicit table reference
```

## Future Enhancement Areas

### Potential Improvements
1. **Parallel processing:** Multi-threading for batch processing
2. **Better matching:** Machine learning for match scoring
3. **Incremental updates:** Only process new records
4. **Result validation:** Automated quality checks
5. **API wrapper:** RESTful API for matching service

### Extensibility Points
- Custom string normalization rules
- Alternative fuzzy matching algorithms
- Additional demographic filters
- Multiple voter data sources
- Real-time matching capabilities

## Getting Help

When stuck:
1. Check function docstrings for parameter details
2. Review usage examples in `matching_students.py`
3. Examine test cases (if available)
4. Refer to library documentation (Ibis, Polars, DuckDB)

## Version History

- **Current:** Initial system with batch processing and geocoding
- **Planned:** Performance optimizations and ML-based matching

---

**Note for AI Agents:** This codebase prioritizes correctness and memory efficiency over speed. Maintain these priorities when making modifications. Always test with sample data before processing full datasets.
