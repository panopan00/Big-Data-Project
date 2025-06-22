# DFQ3csv.py

# imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import DoubleType

username = "panagiotispagotelis"
spark = SparkSession \
    .builder \
    .appName("Big Data - 5. DF Query 3 (csv)") \
    .getOrCreate()
sc = spark.sparkContext

# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
sc.setLogLevel("ERROR")

# Λήψη του job ID και καθορισμός της διαδρομής εξόδου
job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/DFQ3csv_{job_id}"

# Καθορισμός raw_data_path
raw_data_path = "hdfs://hdfs-namenode:9000/user/root/data"

#--- Βήμα 1: Φόρτωση δεδομένων csv ---
census_df = spark.read.csv(
    f"{raw_data_path}/2010_Census_Populations_by_Zip_Code.csv",
    header=True,
    inferSchema=True
)

income_df = spark.read.csv(
    f"{raw_data_path}/LA_income_2015.csv",
    header=True,
    inferSchema=True
)

#--- Βήμα 2: Καθαρισμός και μετατροπή τύπων ---
income_df_clean = income_df.withColumn(
    "Median_Income_Num",
    regexp_replace(col("Estimated Median Income"), "[$,]", "").cast(DoubleType())
)

#--- Βήμα 3: Join των DataFrames ---
joined_df = census_df.join(
    income_df_clean,
    'Zip Code',
    how='inner'
)

#--- Βήμα 4: Υπολογισμός εισοδήματος ανά άτομο ---
result_df = joined_df.withColumn(
    "income_per_capita",
    col("Median_Income_Num") / (col("Total Population") / col("Total Households"))
).filter(
    (col("Total Population") > 0) & (col("Total Households") > 0)
).select(
    col("Zip Code"),
    "income_per_capita"
).orderBy(col("income_per_capita").desc())

print("--- Αποτέλεσμα Query 3 (DataFrame API - CSV) ---")
result_df.show()

spark.stop()