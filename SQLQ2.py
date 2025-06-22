# SQLQ2.py

#imports
from pyspark.sql import SparkSession

username = "panagiotispagotelis"
spark = SparkSession \
    .builder \
    .appName("Big Data - 4. SQL Query 2") \
    .getOrCreate()
sc = spark.sparkContext

# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
sc.setLogLevel("ERROR")

# Λήψη του job ID και καθορισμός της διαδρομής εξόδου
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/SQLQ2_{job_id}"

# Καθορισμός base_path
hdfs_base_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet"

#--- Φόρτωση δεδομένων ---
crime_df_1 = spark.read.parquet(f"{hdfs_base_path}/LA_Crime_Data_2010_2019")
crime_df_2 = spark.read.parquet(f"{hdfs_base_path}/LA_Crime_Data_2020_2025")
crimes_df = crime_df_1.union(crime_df_2)

# --- Δημιουργία view για να γίνει το query ---
crimes_df.createOrReplaceTempView("crimes")

# --- SQL Query ---
sql_query = """
    WITH precinct_rates AS (
        SELECT
            YEAR(TO_DATE(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) as year,
            `AREA NAME` as precinct,
            SUM(CASE WHEN `Status Desc` IN ('UNK', 'Invest Cont') THEN 0 ELSE 1 END) * 100.0 / COUNT(*) as closed_case_rate
        FROM crimes
        WHERE `AREA NAME` IS NOT NULL 
        GROUP BY year, precinct
    ),
    ranked_precincts AS (
        SELECT
            year,
            precinct,
            closed_case_rate,
            DENSE_RANK() OVER (PARTITION BY year ORDER BY closed_case_rate DESC) as rank
        FROM precinct_rates
    )
    SELECT
        year,
        precinct,
        closed_case_rate,
        rank
    FROM ranked_precincts
    WHERE rank <= 3
    ORDER BY year, rank
"""

# Εκτέλεση του SQL Query
result_df = spark.sql(sql_query)

print("--- Αποτέλεσμα Query 2 (SQL API) ---")
result_df.show(100, truncate=False)

spark.stop()