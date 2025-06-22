# DFQ4.py

# imports
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, count, avg, lower, trim, upper
from pyspark.sql.types import DoubleType, BooleanType

username = "panagiotispagotelis"
spark = SparkSession \
    .builder \
    .appName("Big Data - 6. DF Query 4") \
    .getOrCreate()
sc = spark.sparkContext

# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
sc.setLogLevel("ERROR")

# Λήψη του job ID και καθορισμός της διαδρομής εξόδου
job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/DFQ4_{job_id}"

# Καθορισμός base_path
hdfs_base_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet"

# --- Βήμα 1: Ορισμός UDF για τον υπολογισμό της απόστασης ---
# Θα χρησιμοποιηθεί ο τύπος Haversine για τον υπολογισμό της απόστασης
def haversine_distance(lon1, lat1, lon2, lat2):
    try:
        lon1, lat1, lon2, lat2 = map(math.radians, [float(lon1), float(lat1), float(lon2), float(lat2)])
        R = 6371
        delta_lon = lon2 - lon1
        delta_lat = lat2 - lat1
        a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c
    except (ValueError, TypeError):
        return None

# Εγγραφή της συνάρτησης ως Spark UDF
haversine_udf = udf(haversine_distance, DoubleType())

# --- Βήμα 2: Φόρτωση των MO codes ---
mo_codes_df = spark.read.parquet(f"{hdfs_base_path}/MO_codes")

# --- Βήμα 3: Εύρεση gun και weapon στους MO codes ---
gun_weapon_codes_list = mo_codes_df.filter(
    lower(col("description")).contains("gun") | lower(col("description")).contains("weapon")
).select("code").rdd.flatMap(lambda x: x).collect()

# Broadcast για κάθε worker
broadcast_codes = sc.broadcast(set(gun_weapon_codes_list))

# --- Βήμα 4: UDF για να ελέγχει αν μια σειρά κωδικών περιέχει κάποιον από τους κωδικούς όπλων ---
def has_gun_weapon_code(mocodes_str):
    if not mocodes_str:
        return False
    crime_codes = set(mocodes_str.split())
    return not crime_codes.isdisjoint(broadcast_codes.value)

has_gun_weapon_code_udf = udf(has_gun_weapon_code, BooleanType())

# --- Βήμα 5: Φόρτωση και φιλτράρισμα δεδομένων εγκληματικότητας ---
crime_df_1 = spark.read.parquet(f"{hdfs_base_path}/LA_Crime_Data_2010_2019")
crime_df_2 = spark.read.parquet(f"{hdfs_base_path}/LA_Crime_Data_2020_2025")
crimes_df = crime_df_1.union(crime_df_2)

# --- Βήμα 6: Φιλτράρισμα Null Island και MO codes ---
gun_crimes_df = crimes_df \
    .filter((col("LAT") != 0.0) & (col("LON") != 0.0)) \
    .filter(has_gun_weapon_code_udf(col("Mocodes")))

# --- Βήμα 7: Φόρτωση δεδομένων αστυνομικών τμημάτων ---
stations_df = spark.read.parquet(f"{hdfs_base_path}/LA_Police_Stations") \
    .withColumnRenamed("X", "station_lon") \
    .withColumnRenamed("Y", "station_lat") \
    .withColumnRenamed("DIVISION", "division_name")

# Κλειδιά για το join
gun_crimes_clean_df = gun_crimes_df.withColumn("join_key", trim(upper(col("AREA NAME"))))
stations_clean_df = stations_df.withColumn("join_key", trim(upper(col("division_name"))))

# --- Βήμα 8: Join εγκλημάτων με τμήματα ---
# Το κλειδί είναι το όνομα της περιοχής (AREA NAME) και του τμήματος (division_name)
joined_df = gun_crimes_clean_df.join(
    stations_clean_df,
    "join_key",
    how='inner'
)

# --- Βήμα 9: Υπολογισμός απόστασης για κάθε έγκλημα ---
distance_df = joined_df.withColumn("distance_km",
                                   haversine_udf(
                                       col("LON").cast(DoubleType()),
                                       col("LAT").cast(DoubleType()),
                                       col("station_lon").cast(DoubleType()),
                                       col("station_lat").cast(DoubleType())
                                   )
                                   )

# --- Βήμα 10: Ομαδοποίηση και τελικοί υπολογισμοί ---
result_df = distance_df.groupBy("division_name") \
    .agg(
        count("*").alias("#"),
        avg("distance_km").alias("average_distance")
    ) \
    .orderBy(col("#").desc()) \
    .select(
        col("division_name").alias("division"),
        col("average_distance"),
        col("#")
    )

print("--- Αποτέλεσμα Query 4 ---")
result_df.show(truncate=False)

spark.stop()