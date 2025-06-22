# DFQ1.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lower

username = "panagiotispagotelis"
spark = SparkSession \
    .builder \
    .appName("Big Data - 3. DF Query 1") \
    .getOrCreate()
sc = spark.sparkContext

# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
sc.setLogLevel("ERROR")

# Λήψη του job ID και καθορισμός της διαδρομής εξόδου
job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/DFQ1_{job_id}"

# Καθορισμός base_path
hdfs_base_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet"

#--- Βήμα 1: Φόρτωση δεδομένων ---
crime_df_1 = spark.read.parquet(f"{hdfs_base_path}/LA_Crime_Data_2010_2019")
crime_df_2 = spark.read.parquet(f"{hdfs_base_path}/LA_Crime_Data_2020_2025")
crimes_df = crime_df_1.union(crime_df_2)

#--- Βήμα 2: Φιλτράρισμα για "aggravated assault" ---
assaults_df = crimes_df.filter(lower(col("Crm Cd Desc")).contains("aggravated assault"))

#--- Βήμα 3: Κατηγοριοποίηση ηλικιών ---
clean_age = col("Vict Age").cast("int")
age_groups_df = assaults_df.withColumn("Age Group",
    when((clean_age > 0) & (clean_age < 18), "Παιδιά")
    .when((clean_age >= 18) & (clean_age <= 24), "Νεαροί ενήλικοι")
    .when((clean_age >= 25) & (clean_age <= 64), "Ενήλικοι")
    .when(clean_age > 64, "Ηλικιωμένοι")
    .otherwise("Άγνωστο")
)

#--- Βήμα 4: Ομαδοποίηση, καταμέτρηση και ταξινόμηση κατά φθίνουσα σειρά ---
result_df = age_groups_df.filter(col("Age Group") != "Άγνωστο") \
    .groupBy("Age Group") \
    .count() \
    .orderBy(col("count").desc())

print("--- Αποτέλεσμα Query 1 (DataFrame API - Χωρίς UDF) ---")
result_df.show()

spark.stop()