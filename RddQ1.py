# RddQ1.py

#imports
from pyspark.sql import SparkSession

username = "panagiotispagotelis"
spark = SparkSession \
    .builder \
    .appName("Big Data - 3. Rdd Query 1") \
    .getOrCreate()
sc = spark.sparkContext

# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
sc.setLogLevel("ERROR")

# Λήψη του job ID και καθορισμός της διαδρομής εξόδου
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/RddQ1_{job_id}"

# Καθορισμός base_path
hdfs_base_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet"

#--- Βήμα 1: Φόρτωση δεδομένων ---
crime_df_1 = spark.read.parquet(f"{hdfs_base_path}/LA_Crime_Data_2010_2019")
crime_df_2 = spark.read.parquet(f"{hdfs_base_path}/LA_Crime_Data_2020_2025")
crimes_rdd = crime_df_1.rdd.union(crime_df_2.rdd)

#--- Βήμα 2: Φιλτράρισμα για "aggravated assault" ---
assaults_rdd = crimes_rdd.filter(
    lambda row: row['Crm Cd Desc'] is not None and \
                'aggravated assault' in row['Crm Cd Desc'].lower()
)

#--- Βήμα 3: Εξαγωγή και καθαρισμός ηλικιών ---
def parse_age(row):
    try:
        age = int(row['Vict Age'])
        if age > 0: return age
    except (ValueError, TypeError): pass
    return None
ages_rdd = assaults_rdd.map(parse_age).filter(lambda age: age is not None)

#--- Βήμα 4: Κατηγοριοποίηση ηλικιών ---
def categorize_age(age):
    if age < 18: return "Παιδιά"
    elif 18 <= age <= 24: return "Νεαροί ενήλικοι"
    elif 25 <= age <= 64: return "Ενήλικοι"
    else: return "Ηλικιωμένοι"
age_groups_rdd = ages_rdd.map(categorize_age)

#--- Βήμα 5: Καταμέτρηση (Όπως στο Word Count) ---
grouped_counts_rdd = age_groups_rdd.map(lambda group: (group, 1)).reduceByKey(lambda a, b: a + b)

#--- Βήμα 6: Ταξινόμηση κατά φθίνουσα σειρά ---
sorted_counts_rdd = grouped_counts_rdd.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

print("--- Αποτέλεσμα Query 1 (RDD API) ---")
for count, group in sorted_counts_rdd.collect():
    print(f"Ομάδα: {group}, Αριθμός: {count}")

spark.stop()