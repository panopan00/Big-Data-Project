# RddQ3.py

#imports
from pyspark.sql import SparkSession

username = "panagiotispagotelis"
spark = SparkSession \
    .builder \
    .appName("Big Data - 5. Rdd Query 3") \
    .getOrCreate()
sc = spark.sparkContext

# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
sc.setLogLevel("ERROR")

# Λήψη του job ID και καθορισμός της διαδρομής εξόδου
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/RddQ3_{job_id}"

# Καθορισμός base_path
hdfs_base_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet"

#--- Βήμα 1: Φόρτωση και μορφοποίηση δεδομένων απογραφής ---
census_rdd = spark.read.parquet(f"{hdfs_base_path}/2010_Census_Populations_by_Zip_Code").rdd
census_kv_rdd = census_rdd.map(lambda row: (
    row['Zip Code'],
    (row['Total Population'], row['Total Households'])
))

# --- Βήμα 2: Φόρτωση και έλεγχος δεδομένων εισοδήματος ---
income_rdd = spark.read.parquet(f"{hdfs_base_path}/LA_income_2015").rdd

def parse_income(row):
    try:
        income_str = row['Estimated Median Income'].replace('$', '').replace(',', '')
        return row['Zip Code'], float(income_str)
    except (ValueError, TypeError, AttributeError):
        return None

income_kv_rdd = income_rdd.map(parse_income).filter(lambda x: x is not None)

# --- Βήμα 3: Συνένωση (Join) των δύο RDDs βάσει του Zip Code ---
# Αποτέλεσμα: (Zip Code, ((Total Pop, Total Households), Median Income))
joined_rdd = census_kv_rdd.join(income_kv_rdd)

# --- Βήμα 4: Υπολογισμός του εισοδήματος ανά άτομο ---
def calculate_income_per_capita(row):
    zip_code, data = row
    census_data, median_income = data
    total_pop, total_households = census_data

    try:
        if total_households and total_households > 0 and total_pop and total_pop > 0:
            people_per_household = total_pop / total_households
            income_per_capita = median_income / people_per_household
            return zip_code, income_per_capita
    except (TypeError, ZeroDivisionError):
        pass
    return None

income_per_capita_rdd = joined_rdd.map(calculate_income_per_capita).filter(lambda x: x is not None)

# --- Βήμα 5: Ταξινόμηση και εμφάνιση αποτελεσμάτων ---
sorted_rdd = income_per_capita_rdd.sortBy(lambda x: x[1], ascending=False)
top_20_results = sorted_rdd.take(20)

print("--- Αποτέλεσμα Query 3 (RDD API) ---")
print("Zip Code, Income Per Capita")
for row in top_20_results:
    print(f"{row[0]}, {row[1]}")

spark.stop()