# glue_l1_generic.py
import sys
import traceback
from datetime import datetime
from functools import reduce

import boto3

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

sys.path.insert(0, "/tmp")
import common_utils as cu

REQUIRED_ARGS = [
    "JOB_NAME", "DATASET_ID", "DATASET_NAME", "SOURCE_BUCKET", "SOURCE_PREFIX",
    "TARGET_BUCKET", "TARGET_PREFIX", "DB_SECRET_NAME", "AWS_REGION",
]
args = getResolvedOptions(sys.argv, REQUIRED_ARGS)

sc       = SparkContext()
glue_ctx = GlueContext(sc)
spark    = glue_ctx.spark_session
job      = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

s3       = boto3.client("s3", region_name=args["AWS_REGION"])
batch_id = int(datetime.now().timestamp())

dataset_id    = int(args["DATASET_ID"])
dataset_name  = args["DATASET_NAME"]
source_bucket = args["SOURCE_BUCKET"]
source_prefix = args["SOURCE_PREFIX"]
target_bucket = args["TARGET_BUCKET"]
target_prefix = args["TARGET_PREFIX"]
target_path   = f"s3://{target_bucket}/{target_prefix}"
quarantine_base = f"s3://{target_bucket}/_quarantine/{dataset_name}/batch_{batch_id}/"

pg_conn = None

def run():
    global pg_conn
    print(f"=== START dataset={dataset_name} batch_id={batch_id} ===")
    pg_conn = cu.get_pg_conn_from_secret(args["DB_SECRET_NAME"], args["AWS_REGION"])

    meta = cu.load_schema_metadata(pg_conn, dataset_id)
    if not meta:
        raise ValueError(f"No schema metadata found for dataset_id={dataset_id}. Seed dataset_schema_metadata first.")

    spark_schema = cu.build_spark_schema(meta)
    pk_cols      = cu.get_pk_columns(meta)
    if not pk_cols:
        raise ValueError(f"No primary key defined for dataset_id={dataset_id}.")

    print(f"[META] columns={len(meta)} pk={pk_cols}")

    ckpt   = cu.get_checkpoint(pg_conn, dataset_name)
    cutoff = ckpt["last_watermark"]
    print(f"[CKPT] last_watermark={cutoff} last_batch_id={ckpt['last_batch_id']}")

    files = _list_new_files(source_bucket, source_prefix, cutoff)
    if not files:
        print(f"[SKIP] No new files for {dataset_name}")
        return {"status": "SKIPPED", "reason": "no_new_files", "batch_id": batch_id}

    print(f"[FILES] {len(files)} new file(s) to process")

    new_frames     = []
    latest_file_ts = None
    all_drift      = []

    for f in files:
        path      = f"s3://{source_bucket}/{f['Key']}"
        header_df = spark.read.option("header", True).csv(path)

        drift = cu.detect_schema_drift(header_df.columns, meta)
        if drift:
            cu.log_drift(pg_conn, batch_id, dataset_name, f["Key"], drift)
            all_drift.extend(drift)
            print(f"[DRIFT] {f['Key']} → {len(drift)} drift(s)")

        df = spark.read \
            .option("header", True) \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt") \
            .schema(spark_schema) \
            .csv(path)

        if "_corrupt" in df.columns:
            df = df.drop("_corrupt")

        new_frames.append(df)
        if latest_file_ts is None or f["LastModified"] > latest_file_ts:
            latest_file_ts = f["LastModified"]

    new_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=False), new_frames)
    new_df.cache()
    new_rows = new_df.count()
    print(f"[READ] {new_rows} new rows, {len(new_df.columns)} columns")

    cu.start_batch(pg_conn, batch_id, dataset_name, ",".join(f["Key"] for f in files), target_path)

    def ref_reader(ds, col):
        return spark.read.parquet(f"s3://{target_bucket}/silver/{ds}/")

    rules = cu.load_dqm_rules(pg_conn, dataset_id)
    if rules:
        print(f"[DQ] applying {len(rules)} rules")
        passed, results, quar_df = cu.apply_dq_rules(new_df, rules, batch_id, pg_conn, dataset_id, dataset_name, pk_cols, ref_reader)
        cu.write_dq_results(pg_conn, batch_id, dataset_id, dataset_name, results)
    else:
        print(f"[DQ] no rules configured — skipping")
        passed, results, quar_df = True, [], None

    if quar_df is not None:
        try:
            qcount = quar_df.count()
            if qcount > 0:
                quar_df.write.mode("append").parquet(quarantine_base)
                print(f"[QUARANTINE] {qcount} rows → {quarantine_base}")
        except Exception as e:
            print(f"[QUARANTINE-WARN] failed to write: {e}")

    if not passed:
        msg = "Critical DQ rules failed — batch aborted"
        cu.finish_batch(pg_conn, batch_id, "DQ_FAILED", new_rows, msg)
        print(f"[DQ-FAIL] {msg}")
        return {"status": "DQ_FAILED", "batch_id": batch_id}

    try:
        existing_df = spark.read.parquet(target_path)
        existing_count = existing_df.count()
        print(f"[L1] existing rows={existing_count}")
    except Exception:
        existing_df = None
        print(f"[L1] no existing data — first load")

    merged_df = cu.cdc_merge(existing_df, new_df, pk_cols, "updated_at")
    merged_df = merged_df.dropDuplicates(pk_cols)
    merged_df.cache()

    final_rows = merged_df.count()
    print(f"[MERGE] final rows={final_rows} (inserted/updated from {new_rows})")

    merged_df.write.mode("overwrite").parquet(target_path)
    print(f"[WRITE] {target_path}")

    cu.finish_batch(pg_conn, batch_id, "SUCCESS", final_rows)
    cu.update_checkpoint(pg_conn, dataset_name, latest_file_ts, batch_id)

    print(f"=== DONE dataset={dataset_name} batch_id={batch_id} rows={final_rows} drift={len(all_drift)} ===")
    return {"status": "SUCCESS", "batch_id": batch_id, "dataset": dataset_name, "new_rows": new_rows, "final_rows": final_rows, "drift_count": len(all_drift)}

def _list_new_files(bucket, prefix, cutoff):
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            key = o["Key"]
            if not key.endswith(".csv"): continue
            if key.endswith("/"): continue
            if o["Size"] == 0: continue
            if cutoff is not None and o["LastModified"].replace(tzinfo=None) <= cutoff: continue
            files.append(o)
    files.sort(key=lambda x: x["LastModified"])
    return files

try:
    result = run()
    print(f"[RESULT] {result}")
except Exception as e:
    err = traceback.format_exc()[:4000]
    print(f"[ERROR] {err}")
    try:
        if pg_conn is not None:
            cu.finish_batch(pg_conn, batch_id, "FAILED", 0, err)
    except Exception as audit_err:
        print(f"[AUDIT-WARN] could not update batch_audit: {audit_err}")
    raise
finally:
    try:
        if pg_conn is not None:
            pg_conn.close()
    except Exception:
        pass
    job.commit()
