# DFQ2.py

# imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, to_date, count, sum, when, col, rank
from pyspark.sql.window import Window

username = "panagiotispagotelis"
spark = SparkSession \
    .builder \
    .appName("Big Data - 4. DF Query 2") \
    .getOrCreate()
sc = spark.sparkContext

# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
sc.setLogLevel("ERROR")

# Λήψη του job ID και καθορισμός της διαδρομής εξόδου
job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/DFQ2_{job_id}"

# Καθορισμός base_path
hdfs_base_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet"

#--- Βήμα 1: Φόρτωση δεδομένων ---
crime_df_1 = spark.read.parquet(f"{hdfs_base_path}/LA_Crime_Data_2010_2019")
crime_df_2 = spark.read.parquet(f"{hdfs_base_path}/LA_Crime_Data_2020_2025")
crimes_df = crime_df_1.union(crime_df_2)

#--- Βήμα 2: Εξαγωγή έτους και δημιουργία flag για τις κλειστές υποθέσεις ---
processed_df = crimes_df \
    .withColumn("Year", year(to_date(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))) \
    .withColumn("is_closed",
        when(col("Status Desc").isin("UNK", "Invest Cont"), 0).otherwise(1)
    )

#--- Βήμα 3: Ομαδοποίηση και υπολογισμός ποσοστών ---
precinct_stats_df = processed_df \
    .groupBy("Year", "AREA NAME") \
    .agg(
        count("*").alias("total_cases"),
        sum("is_closed").alias("closed_cases")
    ) \
    .withColumn("closed_case_rate", (col("closed_cases") / col("total_cases")) * 100)

#--- Βήμα 4: Χρήση window functions για εύρεση top 3 ---
window_spec = Window.partitionBy("Year").orderBy(col("closed_case_rate").desc())

final_results_df = precinct_stats_df \
    .withColumn("rank", rank().over(window_spec)) \
    .filter(col("rank") <= 3) \
    .select(
        col("Year").alias("year"),
        col("AREA NAME").alias("precinct"),
        col("closed_case_rate"),
        col("rank")
    ) \
    .orderBy("year", "rank")

print("--- Αποτέλεσμα Query 2 (DataFrame API) ---")
final_results_df.show(100, truncate=False)

spark.stop()