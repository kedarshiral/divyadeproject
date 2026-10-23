# glue_l0_raw_to_bronze.py
import sys
import json
import boto3
import traceback
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import *

sys.path.insert(0, "/tmp")
import common_utils as cu

# 1. Resolve Arguments
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "RAW_BUCKET", "RAW_PREFIX", 
    "BRONZE_BUCKET", "DB_SECRET_NAME", "AWS_REGION"
])

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

s3 = boto3.client("s3", region_name=args["AWS_REGION"])
batch_id = int(datetime.now().timestamp())
pg_conn = None

def run():
    global pg_conn
    pg_conn = cu.get_pg_conn_from_secret(args["DB_SECRET_NAME"], args["AWS_REGION"])
    
    raw_bucket = args["RAW_BUCKET"]
    raw_prefix = args["RAW_PREFIX"].rstrip('/') + '/'
    bronze_bucket = args["BRONZE_BUCKET"]

    # --- 2. Get List of JSON Files from Client Location ---
    response = s3.list_objects_v2(Bucket=raw_bucket, Prefix=raw_prefix)
    files = [obj for obj in response.get("Contents", []) if obj["Key"].endswith(".json")]

    if not files:
        print("[SKIP] No new JSON files found in client location.")
        return

    for file in files:
        file_key = file["Key"]
        file_name = file_key.split("/")[-1]
        
        print(f"=== Processing: {file_name} ===")
        
        # Start Audit
        with pg_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO l0_batch_audit (batch_id, source_file_name, status)
                VALUES (%s, %s, 'STARTED')
            """, (batch_id, file_name))
        pg_conn.commit()

        try:
            # --- 3. Read Nested JSON ---
            # Glue me directly S3 se read karne ke liye multiline true zaroori hai
            df_raw = spark.read.option("multiLine", True).json(f"s3://{raw_bucket}/{file_key}")
            
            # Note: Assuming JSON structure is like:
            # { "customer": {...}, "order": {...}, "payment": {...} }
            # Agar array hai like [{...}, {...}], to DataFrame me directly columns milenge

            # --- 4. Explode into 3 DataFrames ---
            # Customer
            if "customer" in df_raw.columns:
                df_customer = df_raw.select("customer.*")
            else:
                df_customer = spark.createDataFrame([], StructType([]))
                
            # Order
            if "order" in df_raw.columns:
                df_order = df_raw.select("order.*")
            else:
                df_order = spark.createDataFrame([], StructType([]))
                
            # Payment
            if "payment" in df_raw.columns:
                df_payment = df_raw.select("payment.*")
            else:
                df_payment = spark.createDataFrame([], StructType([]))

            # --- 5. Write to Bronze as CSV ---
            cust_count, order_count, pay_count = 0, 0, 0
            
            # Helper to write CSV
            def write_bronze(df, dataset_name):
                path = f"s3://{bronze_bucket}/bronze/{dataset_name}/batch_{batch_id}"
                df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)
                return df.count()

            if df_customer.count() > 0: cust_count = write_bronze(df_customer, "customer")
            if df_order.count() > 0: order_count = write_bronze(df_order, "order")
            if df_payment.count() > 0: pay_count = write_bronze(df_payment, "payment")

            # --- 6. Archive Raw JSON ---
            now = datetime.now()
            archive_key = f"archive/{now.strftime('%Y/%m/%d/%H%M%S')}/{file_name}"
            
            s3.copy_object(
                Bucket=bronze_bucket,
                CopySource={'Bucket': raw_bucket, 'Key': file_key},
                Key=archive_key
            )
            # Delete original after successful copy
            s3.delete_object(Bucket=raw_bucket, Key=file_key)

            # --- 7. Update Audit Success ---
            with pg_conn.cursor() as cur:
                cur.execute("""
                    UPDATE l0_batch_audit 
                    SET status = 'SUCCESS', 
                        customer_rows = %s, order_rows = %s, payment_rows = %s,
                        archive_path = %s, completed_at = NOW()
                    WHERE batch_id = %s
                """, (cust_count, order_count, pay_count, f"s3://{bronze_bucket}/{archive_key}", batch_id))
            pg_conn.commit()
            
            print(f"[SUCCESS] {file_name} archived and loaded. C:{cust_count} O:{order_count} P:{pay_count}")

        except Exception as e:
            err = traceback.format_exc()[:2000]
            print(f"[ERROR] Failed to process {file_name}: {err}")
            
            with pg_conn.cursor() as cur:
                cur.execute("""
                    UPDATE l0_batch_audit 
                    SET status = 'FAILED', error_message = %s, completed_at = NOW()
                    WHERE batch_id = %s
                """, (err, batch_id))
            pg_conn.commit()
            raise e

try:
    run()
except Exception as e:
    raise e
finally:
    if pg_conn: pg_conn.close()
    job.commit()
