# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,FloatType,DateType
from pyspark.sql.functions import col, concat, current_timestamp,regexp_replace,lit,to_date,sha2


# COMMAND ----------

# MAGIC %run "/Users/sivajujare@outlook.com/config_files/Env_config"

# COMMAND ----------

payment_schema = StructType(fields=[StructField("loan_id", StringType(), False),
                                     StructField("mem_id", StringType(), False),
                                     StructField("latest_transaction_id", StringType(), False),
                                     StructField("funded_amnt_inv", DoubleType(), True),
                                     StructField("total_pymnt_rec", FloatType(), True),
                                     StructField("installment", FloatType(), True),
                                     StructField("last_pymnt_amnt", FloatType(), True),
                                     StructField("last_pymnt_d", DateType(), True),
                                     StructField("next_pymnt_d", DateType(), True),
                                     StructField("pymnt_method", StringType(), True)
                                     
])

# COMMAND ----------


dbutils.fs.cp(
    f"{raw_files}loan_payment.csv",
    f"{source_files}loan-payments/loan_payments_{_date}.csv"

)

# COMMAND ----------

ln_pey_df = spark.read \
                    .option("header", True) \
                    .schema(payment_schema) \
                    .csv(f"{source_files}loan-payments/loan_payments_{_date}.csv")

#display(ln_pey_df)                  

# COMMAND ----------

date_df = ingest_date(ln_pey_df)
add_col_df = date_df.withColumn("payment_key", sha2(concat(col("loan_id"), col("mem_id"), col("latest_transaction_id")), 256))
#display(add_col_df)

# COMMAND ----------

replace_null_df = add_col_df.replace("null", None)
renamed_df = replace_null_df.withColumnRenamed("mem_id","member_id") \
.withColumnRenamed("funded_amnt_inv","funded_amount_investor") \
.withColumnRenamed("total_pymnt_rec","total_payment_recorded") \
.withColumnRenamed("last_pymnt_amnt","last_payment_amount") \
.withColumnRenamed("last_pymnt_d","last_payment_date") \
.withColumnRenamed("next_pymnt_d","next_payment_date") \
.withColumnRenamed("pymnt_method","payment_method") 
#display(renamed_df)

# COMMAND ----------

renamed_df.createOrReplaceTempView("pay_temp")
final_df = spark.sql("select payment_key,ingest_date,loan_id,member_id,latest_transaction_id,funded_amount_investor,total_payment_recorded, installment,last_payment_amount,last_payment_date,next_payment_date,payment_method from pay_temp")
#display(final_df)

# COMMAND ----------

final_df.write \
        .options(header = True) \
        .mode("append") \
        .parquet(f"{cleaned_files}loan-payments/")

# COMMAND ----------

dbutils.notebook.exit("Success")