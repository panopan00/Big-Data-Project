# RddQ2.py

#imports
from pyspark.sql import SparkSession

username = "panagiotispagotelis"
spark = SparkSession \
    .builder \
    .appName("Big Data - 4. Rdd Query 2") \
    .getOrCreate()
sc = spark.sparkContext

# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
sc.setLogLevel("ERROR")

# Λήψη του job ID και καθορισμός της διαδρομής εξόδου
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/RddQ2_{job_id}"

# Καθορισμός base_path
hdfs_base_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet"

#--- Βήμα 1: Φόρτωση δεδομένων ---
crime_df_1 = spark.read.parquet(f"{hdfs_base_path}/LA_Crime_Data_2010_2019")
crime_df_2 = spark.read.parquet(f"{hdfs_base_path}/LA_Crime_Data_2020_2025")
crimes_rdd = crime_df_1.rdd.union(crime_df_2.rdd)

# --- Βήμα 2: Εξαγωγή απαραίτητων πεδίων και καθαρισμός ---
# (Year, AREA_NAME, is_closed_flag)
def parse_crime_record(row):
    try:
        date_parts = row['DATE OCC'].split('/')
        year = int(date_parts[2][:4])  # MM/DD/YYYY

        area_name = row['AREA NAME'].strip()
        if not area_name:
            return None

        status = row['Status Desc'].strip()
        is_closed = 0 if status in ('UNK', 'Invest Cont') else 1

        return (year, area_name), (is_closed, 1)
    except:
        return None

parsed_rdd = crimes_rdd.map(parse_crime_record).filter(lambda x: x is not None)

# --- Βήμα 3: Υπολογισμός συνολικών και κλειστών υποθέσεων ---
agg_rdd = parsed_rdd.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# --- Βήμα 4: Υπολογισμός ποσοστού και αναδιάταξη ανά έτος ---
# ((Year, Area), (closed, total)) -> (Year, (Area, rate))
def calculate_rate(row):
    key, value = row
    year, area = key
    closed, total = value
    rate = (closed / total) * 100 if total > 0 else 0
    return year, (area, rate)

rates_by_year_rdd = agg_rdd.map(calculate_rate)

# --- Βήμα 5: Ομαδοποίηση ανά έτος και ταξινόμηση για την κατάταξη (top 3) ---
def get_top_3(area_rate_list):
    sorted_list = sorted(list(area_rate_list), key=lambda x: x[1], reverse=True)
    return [(item[0], item[1], rank + 1) for rank, item in enumerate(sorted_list[:3])]

# groupByKey για να μαζέψουμε όλα τα τμήματα για κάθε έτος
# flatMap για τα top 3 precincts κάθε έτους σε ένα ενιαίο RDD
top_3_rdd = rates_by_year_rdd.groupByKey().flatMap(
    lambda x: [(x[0], precinct_info[0], precinct_info[1], precinct_info[2]) for precinct_info in get_top_3(x[1])]
)

# --- Βήμα 6: Τελική ταξινόμηση για εμφάνιση ---
# (Year, Precinct, Rate, Rank)
final_sorted_rdd = top_3_rdd.sortBy(lambda x: (x[0], x[3]))  # Ταξινόμηση κατά έτος και μετά κατά rank

print("--- Αποτέλεσμα Query 2 (RDD API) ---")
results = final_sorted_rdd.collect()
print("year, precinct, closed_case_rate, #")
for res in results:
    print(f"{res[0]}, {res[1]}, {res[2]:}, {res[3]}")

spark.stop()