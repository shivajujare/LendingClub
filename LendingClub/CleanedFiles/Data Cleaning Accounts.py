# Databricks notebook source
# MAGIC %md
# MAGIC #Data cleaning of Account Details

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,FloatType,DateType
from pyspark.sql.functions import col, concat, current_timestamp,regexp_replace,lit,to_date,when,sha2


# COMMAND ----------

# MAGIC %run "/Users/sivajujare@outlook.com/config_files/Env_config"

# COMMAND ----------

Account_schema = StructType(fields =[ StructField("acc_id", StringType(), False),
                                     StructField("mem_id", StringType(), False),
                                     StructField("loan_id", StringType(), False),
                                     StructField("grade", StringType(), True),
                                     StructField("sub_grade",StringType(), True),
                                     StructField("emp_title",StringType(), True),
                                     StructField("emp_length",StringType(), True),
                                     StructField("home_ownership",StringType(), True),
                                     StructField("annual_inc",FloatType(), True),
                                     StructField("verification_status",StringType(), True),
                                     StructField("tot_hi_cred_lim",FloatType(), True),
                                     StructField("application_type",StringType(), True),
                                     StructField("annual_inc_joint",FloatType(), True),
                                     StructField("verification_status_joint",StringType(), True)
                                ])

# COMMAND ----------

dbutils.fs.cp(
    f"{raw_files}account_details.csv",
    f"{source_files}account-details/account_details_{_date}.csv"
)

# COMMAND ----------

account_df = spark.read \
                .option("header", True) \
                .schema(Account_schema) \
                .csv(f"{source_files}account-details/account_details_{_date}.csv")

# COMMAND ----------

#account_df.createOrReplaceTempView("temp")
#temp_sql = spark.sql("select distinct emp_length from temp").show()

# COMMAND ----------

remove_years_df = account_df.withColumn("emp_length", when(col("emp_length") == lit("n/a"), lit("null"))
                                        .when(col("emp_length") == lit("10+ years"), lit("10"))
                                        .when(col("emp_length") == lit("1 year"), lit("1"))
                                        .when(col("emp_length")== lit("< 1 year"), lit("1"))
                                        .otherwise(col("emp_length")))
#display(remove_years_df)

# COMMAND ----------

remove_years = remove_years_df.withColumn("emp_length", regexp_replace(remove_years_df["emp_length"], " years", ""))
#remove_years.createOrReplaceTempView("temp")
#uniq_values = spark.sql("select distinct emp_length from temp").show()

# COMMAND ----------

replace_null = remove_years.replace("null", None)
#replace_null.createOrReplaceTempView("temp")
#res = spark.sql("select * from temp where emp_length is null").show()

# COMMAND ----------

add_date_df = ingest_date(replace_null)# replace_null.withColumn("ingest_date", current_timestamp())
#display(add_date_df)

# COMMAND ----------

id_key = add_date_df.withColumn("Account_Key", sha2(concat(col("acc_id"),col("mem_id"),col("loan_id")), 256))
#display(id_key)

# COMMAND ----------

account_df_rename=id_key.withColumnRenamed("acc_id","account_id") \
.withColumnRenamed("mem_id","member_id") \
.withColumnRenamed("emp_title","employee_designation") \
.withColumnRenamed("emp_length","employee_experience") \
.withColumnRenamed("annual_inc","annual_income") \
.withColumnRenamed("tot_hi_cred_lim","total_high_credit_limit") \
.withColumnRenamed("annual_inc_joint","annual_income_joint")
#display(account_df_rename)

# COMMAND ----------

account_df_rename.createOrReplaceTempView("temp_table")
final_df=spark.sql("select account_key,ingest_date,account_id,member_id,loan_id,grade,sub_grade,employee_designation,employee_experience,home_ownership,annual_income,verification_status,total_high_credit_limit,application_type,annual_income_joint,verification_status_joint from temp_table ")
#display(final_df)

# COMMAND ----------

#final_df.write.options(header='True').mode("append").parquet("/mnt/finstore2023/lending-club/cleaned-files/acc/")
final_df.write.options(header = 'True').mode("append").parquet(f"{cleaned_files}account-details/")

# COMMAND ----------

#dbutils.fs.ls("/mnt/finstore2023/lending-club/")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

