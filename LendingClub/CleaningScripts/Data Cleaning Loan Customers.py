# Databricks notebook source
# MAGIC %md
# MAGIC #Data Cleaning of Loan Customers

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import  concat, current_timestamp,sha2, col

# COMMAND ----------

# MAGIC %run "/Users/sivajujare@outlook.com/config_files/Env_config"

# COMMAND ----------

dbutils.fs.cp(
    f"{raw_files}loan_customer_data.csv",
    f"{source_files}/loan-customers/loan_customers_{_date}.csv"
)

# COMMAND ----------

customer_schema = StructType(fields=[StructField("cust_id", StringType(), True),
                                     StructField("mem_id", StringType(), True),
                                     StructField("fst_name", StringType(), False),
                                     StructField("lst_name", StringType(), False),
                                     StructField("prm_status", StringType(), False),
                                     StructField("age", IntegerType(), False),
                                     StructField("state", StringType(), False),
                                     StructField("country", StringType(), False)
                                    
])
cust_df = spark.read \
                .option("header", True) \
                .schema(customer_schema) \
                .csv(f"{source_files}/loan-customers/loan_customers_{_date}.csv")
#display(cust_df)                

# COMMAND ----------

ingest_date_df = ingest_date(cust_df)
add_col = ingest_date_df.withColumn("customer_key", sha2(concat(col("mem_id"),col("age"),col("state")), 256))
#display(add_col)

# COMMAND ----------

renamed_df = add_col.withColumnRenamed("cust_id","customer_id") \
.withColumnRenamed("mem_id","member_id") \
.withColumnRenamed("fst_name","first_name") \
.withColumnRenamed("lst_name","last_name") \
.withColumnRenamed("prm_status","premium_status")

remove_null = renamed_df.replace("null", None)

remove_null.createOrReplaceTempView("cust_temp")

final_df = spark.sql("select customer_key,ingest_date,customer_id,member_id,first_name,last_name,premium_status,age,state,country from cust_temp")
display(final_df)

# COMMAND ----------

final_df.write \
        .options(header = True) \
        .mode("append") \
        .parquet(f"{cleaned_files}loan-customers/")

# COMMAND ----------

dbutils.notebook.exit("Success")