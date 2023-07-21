# Databricks notebook source
# MAGIC %md
# MAGIC #Data Cleaning of Investors

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DoubleType, FloatType
from pyspark.sql.functions import col, concat, current_timestamp,sha2

# COMMAND ----------

# MAGIC %run "/Users/sivajujare@outlook.com/config_files/Env_config"

# COMMAND ----------

dbutils.fs.cp(
    f"{raw_files}loan_investors.csv",
    f"{source_files}investor-details/loan_investor_{_date}.csv"
)
#dbutils.fs.ls("/mnt/finstore2023/lending-club/source-files/investor-details/")

# COMMAND ----------

investor_schema = StructType(
                    fields= [StructField("investor_loan_id", StringType(), False),
                            StructField("loan_id", StringType(), False),
                            StructField("investor_id", StringType(), False),
                            StructField("loan_funded_amount", FloatType(), False),
                            StructField("investor_type", StringType(), False),
                            StructField("age", IntegerType(), False),
                            StructField("state", StringType(), False),
                            StructField("country", StringType(), False)
                    ]
)

investor_df = spark.read \
                .option("header", True) \
                .schema(investor_schema) \
                .csv(f"{source_files}investor-details/loan_investor_{_date}.csv")
#display(investor_df)

# COMMAND ----------

ingest_date_df = ingest_date(investor_df)
new_columns = ingest_date_df.withColumn("investor_key", sha2(concat(col("investor_loan_id"), col("loan_id"), col("investor_id")), 256))
#display(new_columns)                         

# COMMAND ----------

new_columns.createOrReplaceTempView("investor_temp_table")
final_df=spark.sql("select investor_key,ingest_date,investor_loan_id,loan_id,investor_id,loan_funded_amount,investor_type,age,state,country from investor_temp_table")
#display(final_df)

# COMMAND ----------

final_df.write.options(header = True) \
        .mode("append") \
        .parquet(f"{cleaned_files}investor-details/")
#dbutils.fs.ls("/mnt/finstore2023/lending-club/cleaned-files/investor-details/")        

# COMMAND ----------

dbutils.notebook.exit("Success")