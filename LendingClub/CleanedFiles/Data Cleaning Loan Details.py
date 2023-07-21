# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import  col, concat, current_timestamp,regexp_replace,lit,to_date,when,sha2

# COMMAND ----------

# MAGIC %run "/Users/sivajujare@outlook.com/config_files/Env_config"

# COMMAND ----------

dbutils.fs.cp(
    f"{raw_files}loan_details.csv",
    f"{source_files}loan-details/loan_details_{_date}.csv"
)

# COMMAND ----------

loan_schema = StructType(fields=[StructField("loan_id", StringType(), False),
                                     StructField("mem_id", StringType(), False),
                                     StructField("acc_id", StringType(), False),
                                     StructField("loan_amt", DoubleType(), True),
                                     StructField("fnd_amt", DoubleType(), True),
                                     StructField("term", StringType(), True),
                                     StructField("interest", StringType(), True),
                                     StructField("installment", FloatType(), True),
                                     StructField("issue_date", DateType(), True),
                                     StructField("loan_status", StringType(), True),
                                     StructField("purpose", StringType(), True),
                                     StructField("title", StringType(), True),
                                     StructField("disbursement_method", StringType(), True)
                                    
])

loan_det = spark.read \
            .option("header", True) \
            .schema(loan_schema) \
            .csv(f"{source_files}loan-details/loan_details_{_date}.csv")
#display(loan_det)

# COMMAND ----------

clean_df = loan_det.withColumn("term", regexp_replace(loan_det["term"], " months", "")) \
                   .withColumn("interest", regexp_replace(loan_det["interest"], "%", ""))
#display(clean_df)

# COMMAND ----------

ingest_date_df = ingest_date(clean_df)
add_col = ingest_date_df.withColumn("loan_key", sha2(concat(col("loan_id"), col("mem_id"), col("acc_id")), 256))
#display(add_col)

# COMMAND ----------

remove_null = add_col.replace("null", None)
rename_df = remove_null.withColumnRenamed("mem_id","member_id") \
.withColumnRenamed("acc_id","account_id") \
.withColumnRenamed("loan_amt","loan_amount") \
.withColumnRenamed("fnd_amt","funded_amount") 
rename_df.createOrReplaceTempView("loan_temp")
#query1 = spark.sql("select * from loan_temp where term=36 and interest > 5.0").show()

# COMMAND ----------

final_df = spark.sql("select loan_key, ingest_date,loan_id,member_id,account_id,loan_amount,funded_amount,term,interest,installment,issue_date,loan_status,purpose,title,disbursement_method from loan_temp")
#display(final_df)

# COMMAND ----------

final_df.write \
        .options(header = True) \
        .mode("append") \
        .parquet(f"{cleaned_files}loan-details/")

# COMMAND ----------

dbutils.notebook.exit("Success")