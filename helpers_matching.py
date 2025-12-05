import os

# from unidecode import unidecode
# import re
from time import sleep as pause

import censusbatchgeocoder
import ibis
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from polars import col
from rapidfuzz import fuzz, process
from tqdm import tqdm

@ibis.udf.scalar.pyarrow
def normalize_string(s: str) -> str:
    if s is None:
        return None
    s = pc.utf8_normalize(s, form='NFD')
    return s
    
def clean_string(col):
    pattern = r'(?i)\b(Jr|Sr|I|II|III|IV|V|PhD|MD|DDS|DVM|JD|DO|OD|PsyD|MBA|CPA|CFA|Esq|Prof|Professor|Dr|Doctor|Mr|Mrs|Ms|Miss|Mx|in\s+remembrance|fics|facs|mrcsedophth|bsc|phdc|msc|bsc|phdc|msc|cfi|cipm|cpp|psp|pci|csmp|chpa|cpted|fismi|cfo|coo|cma|erp|leed|ap|fmp|sfp|cid|cltc|cfs|cfbs|cis|lacp|lutcf|rfc|cae|ncarb|aia|mph|mhl|frcs|fccp|fccm|cipd|icf|leadership\s+coach|msra|msppm|rn|mha|bsn|bba|cnor|cnml|ngo|cia|cissp|cism|ceh|cisa|cbci|ccsa|crisc|frcs|mha|cpe|faapl|kstj|fcpa|fcma|frss|faia|ceng|cenv|aicpa|fiplante|facc|pc|abr|cdpe|clhms|crs|gri|epro)\b|[^a-zA-Z ]'
    new_col = normalize_string(col).re_replace(pattern, "").strip().lower()
    return new_col

@ibis.udf.scalar.builtin(name="jaro_winkler_similarity")
def jw_sim(a: str, b: str) -> float:
    """
    Returns the Jaro-Winkler similarity between two strings using DuckDB's native
    implementation. This function is a built-in UDF and is not executed in Python.
    Its function body is ignored by Ibis and only serves as a placeholder.
    """
    ...

def fuzzy_score(col1, col2):
    """
    Compute a fuzzy similarity score between two string columns.
    
    This wrapper calls the native DuckDB Jaro-Winkler similarity function
    (exposed as jw_sim) and multiplies the result by 100.
    
    Parameters
    ----------
    col1 : ibis.expr.types.ColumnExpr
        The first string column.
    col2 : ibis.expr.types.ColumnExpr
        The second string column.
    
    Returns
    -------
    ibis.expr.types.ColumnExpr
        The fuzzy similarity score as a percentage.
    """
    return 100 * jw_sim(col1, col2)

def process_data_students(students_file, voters_file, birth_year_tol):

    students_table = ibis.read_parquet(students_file)
    voters_table = ibis.read_parquet(voters_file)

    voters_table = voters_table.mutate(
        Voters_BirthYear=ibis.case()
        .when(voters_table["Voters_BirthDate"].isnull(), None)
        .else_(voters_table["Voters_BirthDate"].substr(-4, 4).cast("int64"))
        .end()
        .cast("int64"),
        merge_var=clean_string(voters_table["Voters_FirstName"]),
        merge_var2=clean_string(voters_table["Voters_LastName"]),
        mi1=clean_string(voters_table["Voters_MiddleName"]).lower().substr(0, 1),
        fn1=clean_string(voters_table["Voters_FirstName"]),
        ln1=clean_string(voters_table["Voters_LastName"]),
    )

    students_table = students_table.mutate(
        name=clean_string(students_table["name"]),
        firstname=clean_string(students_table["firstname"]),
        middlename=clean_string(students_table["middlename"]),
        lastname=clean_string(students_table["lastname"]),
    )

    students_table = students_table.mutate(
        merge_var=students_table["firstname"],
        merge_var2=students_table["lastname"],
    )

    joined_data = students_table.join(
        voters_table,
        predicates=[
            students_table.merge_var.substr(0, 1)  == voters_table.merge_var.substr(0, 1),
            students_table.merge_var.substr(-1, 1) == voters_table.merge_var.substr(-1, 1),
            students_table.merge_var2.substr(0, 1)  == voters_table.merge_var2.substr(0, 1),
            fuzzy_score(students_table.merge_var, voters_table.merge_var) > 90,
            fuzzy_score(students_table.merge_var2, voters_table.merge_var2) > 90,
            (
                (voters_table["Voters_BirthYear"] > students_table["cohort"] - 21 - birth_year_tol) 
                & 
                (voters_table["Voters_BirthYear"] < students_table["cohort"] - 21 + birth_year_tol)
            )
        ],
        how="inner",
    )

    # # Construct the filter conditions
    # c1 = (
    #     (voters_table["Voters_BirthYear"] > students_table["cohort"] - 21 - birth_year_tol) 
    #     & 
    #     (voters_table["Voters_BirthYear"] < students_table["cohort"] - 21 + birth_year_tol)
    # )
    # # Apply the filter
    # filtered_data = joined_data.filter(c1)

    final_data = joined_data.mutate(
        middle_initial_flag=(
            joined_data["mi1"] == joined_data["middlename"].substr(0, 1)
        )
        .cast("int8")
        .fill_null(0),
        fuzzy_score_first=fuzzy_score(joined_data["firstname"], joined_data["fn1"]),
        fuzzy_score_last=fuzzy_score(joined_data["lastname"], joined_data["ln1"]),
    ).drop(
        [
            "merge_var",
            "merge_var2",
            "mi1",
            "fn1",
            "ln1",
            "Voters_Active",
            "Voters_OfficialRegDate",
            "Voters_PlaceOfBirth",
        ]
    )

    return final_data#.filter(final_data["fuzzy_score"] >= 85.0)


def process_data_parents(parents_file, voters_file, birth_year_tol):
    parents_table = ibis.read_parquet(parents_file)
    voters_table = ibis.read_parquet(voters_file)

    voters_table = voters_table.mutate(
        Voters_BirthYear=ibis.case()
        .when(voters_table["Voters_BirthDate"].isnull(), None)
        .else_(voters_table["Voters_BirthDate"].substr(-4, 4).cast("int64"))
        .end()
        .cast("int64"),
        merge_var=clean_string(voters_table["Voters_LastName"]),
        merge_var2=clean_string(voters_table["Voters_FirstName"]).lower(),
    )
    parents_table = parents_table.mutate(
        merge_var=clean_string(parents_table["lastname"]),
        merge_var2=clean_string(parents_table["firstname"]).lower(),
    )
    joined_data = parents_table.join(
        voters_table,
        predicates=[
            parents_table.merge_var.substr(0, 1)  == voters_table.merge_var.substr(0, 1),
            parents_table.merge_var.substr(-1, 1) == voters_table.merge_var.substr(-1, 1),
            parents_table.merge_var2.substr(0, 1)  == voters_table.merge_var2.substr(0, 1),
            fuzzy_score(parents_table.merge_var, voters_table.merge_var) > 90,
            fuzzy_score(parents_table.merge_var2, voters_table.merge_var2) > 90,

        ],
        how="inner",
    )

    # Construct the filter conditions
    c1 = (
        joined_data["Voters_BirthYear"] > (joined_data["birth_cohort"] - birth_year_tol)
    ) & (
        joined_data["Voters_BirthYear"] < (joined_data["birth_cohort"] + birth_year_tol)
    )
    c2 = joined_data["birth_cohort"].isnull()

    # Apply the filter
    filtered_data = joined_data.filter(c1 | c2)

    final_data = filtered_data.mutate(
        middle_initial_flag=(
            (
                clean_string(filtered_data["Voters_MiddleName"]).lower().substr(0, 1)
                == clean_string(filtered_data["middlename"]).lower().substr(0, 1)
            )
            .cast("int8")
            .fill_null(0)
        ),
        fuzzy_score=fuzzy_score(
            clean_string(filtered_data["firstname"]),
            clean_string(filtered_data["Voters_FirstName"]),
        ),
        relative_name=clean_string(filtered_data["relative_name"]),
        firstname=clean_string(filtered_data["firstname"]),
        middlename=clean_string(filtered_data["middlename"]),
        lastname=clean_string(filtered_data["lastname"]),
    ).drop(
        [
            "merge_var",
            "merge_var2",
            "Voters_Active",
            "Voters_OfficialRegDate",
            "Voters_PlaceOfBirth",
        ]
    )

    return final_data


def setup_dir(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    else:
        os.system(f"rm -r {dir_name}*")


def find_parquet_files(folder):
    files = sorted([folder + i for i in os.listdir(folder) if ".parquet" in i])
    return files


def run_matching_in_batches(res_dir, folder, base_file, type, birth_year_tol=5):

    setup_dir(res_dir)
    files = find_parquet_files(folder)

    for file in tqdm(files):
        if type == "students":
            df = process_data_students(base_file, file, birth_year_tol)
        else:
            df = process_data_parents(base_file, file, birth_year_tol)
        output_path = (
            res_dir + "matches_" + file.replace(folder, "").replace("VM2Uniform--", "")
        )
        df.to_parquet(output_path)


def add_stacked_address(df, type, sfx):
    df = df.with_columns(
        (
            pl.col(f"{type}_addresses_addressline").fill_null("").str.strip_chars()
            + ", "
            + pl.col(f"{type}_addresses_city").fill_null("").str.strip_chars()
            + ", "
            + pl.col(f"{type}_addresses_state").fill_null("").str.strip_chars()
            + ", "
            + pl.col(f"{type}_addresses_zip").fill_null("").str.strip_chars()
        )
        .str.replace(r", ,", ",")
        .alias(f"{sfx}{type}_address")
    )
    return df


def merge_matching_batches(
    folder, output_name, lazy=True, streaming_mode=False
):
    if lazy:
        # a = pl.scan_parquet(f"{folder}*.parquet")
        # a = pl.scan_parquet(f"matches_students_2019_formatted_10_14_2025_aux.parquet")
        a = pl.concat([pl.scan_parquet(folder + i) for i in os.listdir(folder)])
    else:
        # a = pl.read_parquet(f"{folder}*.parquet")
        a = pl.concat([pl.read_parquet(folder + i) for i in os.listdir(folder)])
    name_col_exp = (
        col("name").str.to_titlecase().alias("name")
        if "name" in a.collect_schema().names()
        else col("relative_name").str.to_titlecase().alias("relative_name")
    )

    a = (
        a#.filter(col("fuzzy_score") >= fuzzy_thresh)
        .unique()
        .with_columns(name_col_exp)
        .with_columns(
            firstname=col("firstname").str.to_titlecase(),
            middlename=pl.when(col("middlename") == "")
            .then(None)
            .otherwise(col("middlename").str.to_titlecase()),
            lastname=col("lastname").str.to_titlecase(),
        )
    )

    old_columns = a.collect_schema().names()
    # Create a dictionary for renaming
    rename_map = {
        old: new
        for old, new in zip(
            old_columns,
            ["cf_" + i.lower() for i in old_columns[:8]]
            + ["vf_" + i.lower() for i in old_columns[8:-2]]
            + old_columns[-2:],
        )
    }
    # Apply the renaming
    a = (
        a.rename(rename_map)
        .pipe(add_stacked_address, type="vf_residence", sfx="")
        .pipe(add_stacked_address, type="vf_mailing", sfx="")
        .with_columns(id_number=pl.cum_count("cf_cohort"))
    )

    if lazy:
        a.collect(engine = 'streaming').write_parquet(output_name)
    else:
        a.write_parquet(output_name)


def make_geocode_files(input_filename, output_filename):
    vars_to_keep = [
        "id_number",
        "vf_residence_addresses_addressline",
        "vf_residence_addresses_city",
        "vf_residence_addresses_state",
        "vf_residence_addresses_zip",
    ]
    rename_dict = {
        "id_number": "id_old",
        "vf_residence_addresses_addressline": "address",
        "vf_residence_addresses_city": "city",
        "vf_residence_addresses_state": "state",
        "vf_residence_addresses_zip": "zipcode",
    }

    df = (
        pl.scan_parquet(input_filename)
        .select(vars_to_keep)
        .rename(rename_dict)
        .group_by(["address", "city", "state", "zipcode"])
        .agg(id_old=col("id_old"))
        .sort(["state", "city", "zipcode", "address"])
        .with_columns(id=pl.cum_count("address"))
    )
    df.collect(streaming=True).write_parquet(output_filename)


def gen_geocode_partitions(geocode_dict_file, dir_name):
    setup_dir(dir_name)
    df = pd.read_parquet(geocode_dict_file)

    # Assuming df is your DataFrame
    rows_per_file = 10_000

    # Calculate the number of files needed
    num_files = len(df) // rows_per_file + int(len(df) % rows_per_file != 0)
    partition_files = []

    for i in tqdm(range(num_files)):
        # Calculate the start and end row indices for each chunk
        start_row = i * rows_per_file
        end_row = (i + 1) * rows_per_file

        # Slice the DataFrame
        chunk = df.iloc[start_row:end_row]

        # Define the file name
        file_name = f"{dir_name}partition_{i + 1}.csv"

        # Export the chunk to a CSV file
        chunk.to_csv(file_name, index=False)
        partition_files.append(file_name)
    return partition_files


def combine_partition_files(file_list, output_name):
    a = pl.concat([pl.read_parquet(i) for i in file_list])
    a.write_parquet(output_name)


def run_geocoding_queries_by_batches(files_partitions):
    geocode_partition_files = []
    for path in tqdm(files_partitions):
        success = False
        retries = 10  # Number of retries in case of an error
        attempt = 0

        while not success and attempt < retries:
            try:
                results = censusbatchgeocoder.geocode(path)
                result_path = path.replace("partition", "geocode_results").replace(
                    ".csv", ".parquet"
                )
                pd.DataFrame(results).to_parquet(result_path)
                geocode_partition_files.append(result_path)
                success = True  # Mark success if no exception occurs
            except Exception as e:
                attempt += 1
                print(
                    f"Error processing {path}: {e}. Retrying in 20 seconds... (Attempt {attempt}/{retries})"
                )
                pause(20)

        # If you want to pause before moving to the next file
        pause(5)
    return geocode_partition_files


def make_address_to_id_table(
    id_mapping_filename, geocoding_data_filename, output_filename
):
    mapping = (
        pl.read_parquet(id_mapping_filename)
        .select([col("id").cast(pl.Int64), "id_old"])
        .explode("id_old")
    )
    geo_data = (
        pl.read_parquet(geocoding_data_filename)
        .unique()
        .with_columns(col("id").cast(pl.Int64))
        .sort("id")
        .drop(["address", "city", "state", "zipcode", "id_old"])
    )
    geo_data = geo_data.rename({i: f"geo_{i}" for i in geo_data.columns if i != "id"})
    address_df = (
        mapping.join(geo_data, how="inner", on="id")
        .drop("id")
        .rename({"id_old": "id_number"})
        .sort("id_number")
    )
    address_df.write_parquet(output_filename)


def merge_geo_to_main_data(main_data_filename, geo_data_filename, output_filename):
    con = ibis.duckdb.connect(threads=128)

    main = con.read_parquet(main_data_filename)
    geo_data = con.read_parquet(geo_data_filename)

    final_data = main.left_join(
        geo_data, main["id_number"] == geo_data["id_number"]
    ).drop(main["id_number"])
    write_parquet_batches(final_data, output_filename)


def add_geocoding_for_students(year):
    make_geocode_files(
        f"matches_students_{year}_formatted.parquet",
        f"geocode_students_data_{year}.parquet",
    )
    files_partitions = gen_geocode_partitions(
        f"geocode_students_data_{year}.parquet", f"geocode_students_parts_{year}/"
    )
    geocode_partition_files = run_geocoding_queries_by_batches(files_partitions)
    combine_partition_files(
        geocode_partition_files, f"geocode_results_students_{year}.parquet"
    )
    print("make_address_to_id_table")
    make_address_to_id_table(
        f"geocode_students_data_{year}.parquet",
        f"geocode_results_students_{year}.parquet",
        f"geocode_table_with_id_students_{year}.parquet",
    )
    print("merge_geo_to_main_data")
    merge_geo_to_main_data(
        f"matches_students_{year}_formatted.parquet",
        f"geocode_table_with_id_students_{year}.parquet",
        f"matches_students_{year}_with_geo.parquet",
    )


def make_partitions_by_school_year(input_filename, result_dir):
    print("make_partitions_by_school_year")
    setup_dir(result_dir)
    df = pl.read_parquet(input_filename)
    schools = df.select("cf_school").unique("cf_school").to_numpy().flatten()
    for school in schools:
        df2 = df.filter(col("cf_school") == school)
        cohorts = df2.select("cf_cohort").unique("cf_cohort").to_numpy().flatten()
        for cohort in cohorts:
            df2.filter(col("cf_cohort") == cohort).unique().sort("cf_name").write_csv(
                f"{result_dir}/{school.replace(' ', '_')}_{cohort}.csv"
            )
