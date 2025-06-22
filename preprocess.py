# preprocess.py

# imports
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType

username = "panagiotispagotelis"
spark = SparkSession \
    .builder \
    .appName("Big Data - 2. Data Preprocessing") \
    .getOrCreate()
sc = spark.sparkContext

# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
sc.setLogLevel("ERROR")

# Λήψη του job ID και καθορισμός της διαδρομής εξόδου
job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/preprocess_{job_id}"

# HDFS paths
hdfs_base_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet"
raw_data_path = "hdfs://hdfs-namenode:9000/user/root/data"

#--- Βήμα 1: Αρχεία csv ---
csv_datasets = {
    "LA_Crime_Data_2010_2019": "LA_Crime_Data_2010_2019.csv",
    "LA_Crime_Data_2020_2025": "LA_Crime_Data_2020_2025.csv",
    "LA_Police_Stations": "LA_Police_Stations.csv",
    "LA_income_2015": "LA_income_2015.csv",
    "2010_Census_Populations_by_Zip_Code": "2010_Census_Populations_by_Zip_Code.csv"
}

for name, filename in csv_datasets.items():
    print(f"Processing {name}...")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("escape", "\"") \
        .csv(f"{raw_data_path}/{filename}")

    df.write.parquet(f"{hdfs_base_path}/{name}", mode="overwrite")
    print(f"Saved {name} to Parquet.")

#--- Βήμα 2: Αρχείο txt ---
print("Processing MO_codes.txt...")

# Schema για MO codes
mo_schema = StructType([
    StructField("code", StringType(), nullable=False),
    StructField("description", StringType(), nullable=False)
])

mo_rdd = spark.sparkContext.textFile(f"{raw_data_path}/MO_codes.txt") \
    .filter(lambda x: x.strip()) \
    .map(lambda x: x.split(maxsplit=1)) \
    .filter(lambda x: len(x) == 2) \
    .map(lambda x: Row(code=x[0], description=x[1]))

mo_df = spark.createDataFrame(mo_rdd, schema=mo_schema)

mo_df.write.parquet(f"{hdfs_base_path}/MO_codes", mode="overwrite")
print("Saved MO_codes to Parquet.")

spark.stop()