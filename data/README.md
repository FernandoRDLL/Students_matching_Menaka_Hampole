# Data Directory

This directory is organized to help manage input and output files for the student-voter matching system.

## Directory Structure

```
data/
├── input/          # Place your input data files here
├── output/         # Final results will be saved here
└── intermediate/   # Temporary/intermediate processing files
```

## Usage

### Input Directory (`input/`)
Place your source data files here:
- Student/parent Parquet files (e.g., `total_cohorts_09_22_2025.parquet`)
- Voter registration Parquet files organized in subdirectories
- Any other input data required for matching

### Output Directory (`output/`)
Final matching results will be saved here:
- Formatted match files (e.g., `matches_students_2024_formatted.parquet`)
- Partitioned CSV files by school and cohort
- Geocoded results (if geocoding is enabled)

### Intermediate Directory (`intermediate/`)
This directory stores temporary files during processing:
- Batch processing results
- Geocoding partition files
- Merge intermediate files

**Note:** Files in `intermediate/` can be safely deleted after successful completion of the matching pipeline.

## .gitignore

All Parquet and CSV files in these directories are excluded from version control to avoid committing large data files or sensitive information. Only this README and directory structure are tracked.

## Best Practices

1. **Backup your data:** Always maintain backups of original input files
2. **Clean intermediate files:** Periodically clean the `intermediate/` directory to save disk space
3. **Organize by date:** Consider creating subdirectories with dates for different runs
4. **Document your data:** Keep a separate log of data sources and processing dates
