# Databricks notebook source
# MAGIC %md
# MAGIC #Data Cleaning Loan Defaulters

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,FloatType,DateType
from pyspark.sql.functions import col, concat, current_timestamp,regexp_replace,lit,to_date,when,sha2

# COMMAND ----------

# MAGIC %run "/Users/sivajujare@outlook.com/config_files/Env_config"

# COMMAND ----------

loan_default_schema = StructType(fields=[StructField("loan_id", StringType(), False),
                                     StructField("mem_id", StringType(), False),
                                     StructField("def_id", StringType(), False),
                                     StructField("delinq_2yrs", IntegerType(), True),
                                     StructField("delinq_amnt",FloatType(), True),
                                     StructField("pub_rec",IntegerType(), True),
                                     StructField("pub_rec_bankruptcies",IntegerType(), True),
                                     StructField("inq_last_6mths",IntegerType(), True),
                                     StructField("total_rec_late_fee",FloatType(), True),
                                     StructField("hardship_flag",StringType(), True),
                                     StructField("hardship_type",StringType(), True),
                                     StructField("hardship_length",IntegerType(), True),
                                     StructField("hardship_amount",FloatType(), True)
 
                                    
])

# COMMAND ----------

dbutils.fs.cp(
    f"{raw_files}loan_defaulters.csv",
    f"{source_files}loan-defaulters/loan_defaulters_{_date}.csv"
)

# COMMAND ----------

ldef_df = spark.read \
                .option("header", True) \
                .schema(loan_default_schema) \
                .csv(f"{source_files}loan-defaulters/loan_defaulters_{_date}.csv")
#display(ldef_df)


# COMMAND ----------

date_df = ingest_date(ldef_df)
add_col_df = date_df.withColumn("loan_defaulter_key", sha2(concat(col("loan_id"), col("mem_id"), col("def_id")), 256))
#display(add_col_df)

# COMMAND ----------

replace_null = add_col_df.replace("null", None)
renamed_df = replace_null.withColumnRenamed("mem_id", "member_id") \
.withColumnRenamed("def_id", "loan_default_id") \
.withColumnRenamed("delinq_2yrs", "defaulters_2yrs") \
.withColumnRenamed("delinq_amnt", "defaulters_amount") \
.withColumnRenamed("pub_rec", "public_records") \
.withColumnRenamed("pub_rec_bankruptcies", "public_records_bankruptcies") \
.withColumnRenamed("inq_last_6mths", "enquiries_6mnths") \
.withColumnRenamed("total_rec_late_fee", "late_fee") 
#display(renamed_df)

# COMMAND ----------

renamed_df.createOrReplaceTempView("temp")
final_df = spark.sql("select loan_defaulter_key, ingest_date, loan_id,member_id,loan_default_id,defaulters_2yrs,defaulters_amount,public_records,public_records_bankruptcies,enquiries_6mnths,late_fee,hardship_flag,hardship_type,hardship_length,hardship_amount from temp")
#display(final_df)

# COMMAND ----------

final_df.write \
        .options(header = True) \
        .mode("append") \
        .parquet(f"{cleaned_files}loan-defaulters/")

# COMMAND ----------

dbutils.notebook.exit("Success")