import ibis
from helpers_matching import *

# print('Matching 2024')
# run_matching_in_batches('Full_students_2024_10_14_2025/', '2024_reduced/', 'total_cohorts_09_22_2025.parquet', 'students', 7)
# merge_matching_batches('Full_students_2024_10_14_2025/', 'matches_students_2024_formatted_10_14_2025.parquet')
# add_geocoding_for_students(2024)
# make_partitions_by_school_year('matches_students_2024_with_geo.parquet', 'matches_students_2024/')
# make_partitions_by_school_year('matches_students_2024_formatted_10_14_2025.parquet', 'matches_students_2024_10_15_2025_by_cohort_year/')

# print('Matching 2019')
# run_matching_in_batches('Full_students_2019_10_14_2025/', '2019_reduced/', 'total_cohorts_09_22_2025.parquet', 'students', 7)
merge_matching_batches('Full_students_2019_10_14_2025/', 'matches_students_2019_formatted_10_14_2025.parquet')
# add_geocoding_for_students(2019)
# make_partitions_by_school_year('matches_students_2019_with_geo.parquet', 'matches_students_2019/')
make_partitions_by_school_year('matches_students_2019_formatted_10_14_2025.parquet', 'matches_students_2019_10_15_2025_by_cohort_year/')
# import duckdb
# print('execute')
# con = duckdb.connect('aux.ddb')
# con.execute("""
#     COPY (
#         SELECT *
#         FROM 'Full_students_2019_10_14_2025/*.parquet'
#     ) TO 'matches_students_2019_formatted_10_14_2025_aux.parquet' (FORMAT PARQUET);
# """)

# 'vf_merge_var_right', 'vf_merge_var2_right'

