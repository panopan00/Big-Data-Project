# DFQ1Udf.py

# imports
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col, lower, udf

username = "panagiotispagotelis"
spark = SparkSession \
    .builder \
    .appName("Big Data - 3. DF Query 1 UDF") \
    .getOrCreate()
sc = spark.sparkContext

# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
sc.setLogLevel("ERROR")

# Λήψη του job ID και καθορισμός της διαδρομής εξόδου
job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/DFQ1Udf_{job_id}"

# Καθορισμός base_path
hdfs_base_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet"

#--- Βήμα 1: Φόρτωση δεδομένων ---
crime_df_1 = spark.read.parquet(f"{hdfs_base_path}/LA_Crime_Data_2010_2019")
crime_df_2 = spark.read.parquet(f"{hdfs_base_path}/LA_Crime_Data_2020_2025")
crimes_df = crime_df_1.union(crime_df_2)

#--- Βήμα 2: Ορισμός UDF ---
def categorize_age(age):
    try:
        age = int(age)
        if age > 0:
            if age < 18: return "Παιδιά"
            elif 18 <= age <= 24: return "Νεαροί ενήλικοι"
            elif 25 <= age <= 64: return "Ενήλικοι"
            else: return "Ηλικιωμένοι"
    except (ValueError, TypeError): pass
    return "Άγνωστο"

#--- Βήμα 3: Συνάρτηση categorize ως Spark UDF ---
categorize_age_udf = udf(categorize_age, StringType())

#--- Βήμα 4: Φιλτράρισμα για "aggravated assault" ---
assaults_df = crimes_df.filter(lower(col("Crm Cd Desc")).contains("aggravated assault"))

#--- Βήμα 5: Εφαρμογή της UDF για δημιουργία νέας στήλης (Age Group) ---
age_groups_df = assaults_df.withColumn("Age Group", categorize_age_udf(col("Vict Age")))

#--- Βήμα 6: Ομαδοποίηση, καταμέτρηση και ταξινόμηση κατά φθίνουσα σειρά ---
result_df = age_groups_df.filter(col("Age Group") != "Άγνωστο") \
    .groupBy("Age Group") \
    .count() \
    .orderBy(col("count").desc())

print("--- Αποτέλεσμα Query 1 (DataFrame API - Με UDF) ---")
result_df.show()

spark.stop()