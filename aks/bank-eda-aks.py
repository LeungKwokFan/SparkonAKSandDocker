# Import the necessary libraries.
# Since we are using Python, import the SparkSession and related functions
# from the PySpark module.
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, format_number, count, when, floor, desc

# Build a SparkSession using the SparkSession APIs.
# If one does not exist, then create an instance. There
# can only be one SparkSession per JVM.
spark = (SparkSession
  .builder
  .appName("bank-eda")
  .getOrCreate())

bank_file = "abfss://sparkstore@ass4.dfs.core.windows.net/bank.csv"


# print(spark.sparkContext.uiWebUrl)

# Get the M&M data set filename from the command-line arguments
# mnm_file = "mnm_dataset.csv"

# Read the file into a Spark DataFrame using the CSV
# format by inferring the schema and specifying that the
# file contains a header, which provides column names for comma-
# separated fields.
bank_df = (spark.read.format("csv") 
  .option("header", "true") 
  .option("inferSchema", "true") 
  .load(bank_file))

print("****************************** Basic Info ***************************************************")

#basic infomation
print(f"Number of rows: {bank_df.count()}")
print(f"Number of columns: {len(bank_df.columns)}")
   
print("****************************** Q1. Expolre whether the marriage will lead to debt **************************")

loan_rate_by_marital = bank_df.groupBy("marital").agg(
    format_number(avg("balance"), 2).alias("avg_balance"),
    (count(when(col("loan") == "yes", True)) / count("*")).alias("loan_yes_rate")
).orderBy(col("loan_yes_rate").desc())

loan_rate_by_marital.show()

overall_loan_rate = round(
    bank_df.where(col("loan") == "yes").count() / bank_df.count(), 2
)
print(f"Overall Loan Rate: {overall_loan_rate:.2%}")

loan_rate_by_marital.write.csv("abfss://sparkstore@ass4.dfs.core.windows.net/bank-test", mode="overwrite", header=True)




print("****************************** Q2. Check the loan rate and housing rate according to different kinds of jobs. And find those with higher housing rate. **************************")

loan_rate_by_job = bank_df.groupBy("job").agg(
    format_number(avg("balance"), 2).alias("avg_balance"),
    (count(when(col("loan") == "yes", True)) / count("*")).alias("loan_yes_rate"),
    (count(when(col("housing") == "yes", True)) / count("*")).alias("housing_yes_rate")
).orderBy(col("housing_yes_rate").desc())

loan_rate_by_job.show()

overall_housing_rate = round(
    bank_df.where(col("housing") == "yes").count() / bank_df.count(), 2
)
print(f"Overall Housing Rate: {overall_housing_rate:.2%}")

loan_rate_by_job.write.csv("abfss://sparkstore@ass4.dfs.core.windows.net/bank-test", mode="overwrite", header=True)


print("****************************** Q3. Explore the relationship between age groups and average deposits and show the housing rates of different age groups. **************************")

age_balance_housing_stats = bank_df \
    .withColumn("age_group", floor(col("age") / 10) * 10) \
    .groupBy("age_group") \
    .agg(
        format_number(avg("balance"), 2).alias("avg_balance"),
        avg(when(col("housing") == "yes", 1).otherwise(0)).alias("housing_rate")
    ) \
    .orderBy(col("avg_balance").desc())


age_balance_housing_stats \
    .where(col("age_group").isin([20, 30, 40, 50, 60])) \
    .show()

age_balance_housing_stats.write.csv("abfss://sparkstore@ass4.dfs.core.windows.net/bank-test", mode="overwrite", header=True)

print("****************************** Q4. Education stat including the distribution of the education situation, the avg balance and housing rate of each education option. ***************************************************")

edu_stats = bank_df.where(col("education") != "unknown") \
                  .groupBy("education") \
                  .agg(count("*").alias("count"),
                       format_number(avg("balance"), 2).alias("avg_balance"),
                       (count(when(col("housing") == "yes", True)) / count("*")).alias("housing_rate")) \
                  .orderBy(desc("avg_balance"), desc("housing_rate"))

edu_stats.show()

edu_stats.write.csv("abfss://sparkstore@ass4.dfs.core.windows.net/bank-test", mode="overwrite", header=True)

print("****************************** Q5. Find those with higher posibility of default according to jobs ***************************************************")

job_default = bank_df.groupBy(
    "job"
).agg(
    count(when(col("default") == "yes", True)).alias("default_count"),
    count(when(col("default") == "no", True)).alias("non_default_count"),
    count("*").alias("total_count")
).withColumn(
    "default_rate", col("default_count") / col("total_count")
).orderBy(desc("default_rate"))

job_default.show()

job_default.write.csv("abfss://sparkstore@ass4.dfs.core.windows.net/bank-test", mode="overwrite", header=True)





spark.stop()



