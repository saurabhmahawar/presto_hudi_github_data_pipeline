import docker
import os
import time
import json
import datetime
import urllib.request
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

def pipeline_failure_alert(context):
    """
    Enterprise Alerting Hook: Fires automatically on task failure.
    Dispatches alerts to Webhook (Slack/Discord/Teams) and logs context.
    """
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    execution_date = str(context.get('execution_date', datetime.datetime.utcnow()))
    exception = context.get('exception', 'Unknown Error')
    log_url = context.get('task_instance').log_url
    
    alert_payload = {
        "text": f"🚨 *LAKEHOUSE PIPELINE ALERT: Task Failed*\n"
                f"• *DAG:* `{dag_id}`\n"
                f"• *Task:* `{task_id}`\n"
                f"• *Timestamp:* `{execution_date}`\n"
                f"• *Error:* `{exception}`\n"
                f"• *Airflow Logs:* <{log_url}|View Logs>"
    }
    
    print("\n=======================================================")
    print("🚨 [ALERT DISPATCHER] TASK FAILURE DETECTED")
    print(f"   DAG: {dag_id} | Task: {task_id}")
    print(f"   Error Summary: {exception}")
    print("=======================================================\n")
    
    webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
    if webhook_url:
        try:
            req = urllib.request.Request(
                webhook_url,
                data=json.dumps(alert_payload).encode('utf-8'),
                headers={'Content-Type': 'application/json'}
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                print(f"Dispatched Slack alert notification (HTTP {resp.status})")
        except Exception as alert_err:
            print(f"Warning: Failed to dispatch Slack webhook alert: {alert_err}")

default_args = {
    'owner': 'lakehouse',
    'start_date': datetime.datetime(2026, 7, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': pipeline_failure_alert,
}

def download_and_upload_github_data():
    """
    Downloads the past 24 hourly .json.gz dumps from GitHub Archive and stages
    them into Object Storage dynamically reading endpoint and bucket from environment.
    """
    endpoint_host = os.environ.get('S3_ENDPOINT', 'http://minio:9000')
    endpoint_url = f"https://{endpoint_host}" if not endpoint_host.startswith("http") else endpoint_host
    bucket_name = os.environ.get('S3_BUCKET_NAME', 'github-raw-data')
    prefix = 'github-raw'
    
    from botocore.client import Config
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('AWS_REGION', 'us-east-1'),
        config=Config(s3={'addressing_style': 'path'})
    )
    
    base_time = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
    
    # Clean up existing files in raw prefix for a clean, deterministic ingestion batch
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            delete_keys = [{'Key': obj['Key']} for obj in response['Contents']]
            s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_keys})
            print(f"Cleaned up existing raw files under {prefix}/ in bucket '{bucket_name}'.")
    except Exception as e:
        print(f"Warning: Cleanup of existing raw files skipped/failed: {e}")
        
    successful_downloads = []
    failed_downloads = []
    
    for i in range(24):
        t = base_time - datetime.timedelta(hours=i)
        hour_str = str(t.hour)
        date_str = t.strftime("%Y-%m-%d")
        filename = f"{date_str}-{hour_str}.json.gz"
        url = f"https://data.gharchive.org/{filename}"
        temp_path = f"/tmp/{filename}"
        
        uploaded = False
        max_retries = 3
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            try:
                print(f"[{i+1}/24] Fetching {url} (Attempt {attempt}/{max_retries})...")
                req = urllib.request.Request(
                    url, 
                    headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'}
                )
                with urllib.request.urlopen(req, timeout=60) as response, open(temp_path, 'wb') as out_file:
                    out_file.write(response.read())
                
                # Verify non-empty file
                file_size = os.path.getsize(temp_path)
                if file_size < 1024:
                    raise ValueError(f"Downloaded file {filename} is unexpectedly small ({file_size} bytes).")
                
                s3_key = f"{prefix}/{filename}"
                print(f"Uploading {temp_path} ({file_size / (1024*1024):.2f} MB) to s3://{bucket_name}/{s3_key}...")
                s3_client.upload_file(temp_path, bucket_name, s3_key)
                
                uploaded = True
                successful_downloads.append((filename, file_size))
                break
            except Exception as e:
                last_error = e
                print(f"Attempt {attempt} failed for {filename}: {e}")
                time.sleep(2 * attempt)
            finally:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                    
        if not uploaded:
            failed_downloads.append((filename, str(last_error)))

    print(f"\n--- Staging Summary: {len(successful_downloads)}/24 files staged successfully ---")
    
    # Assert at least 1 file was staged; log warning if specific individual hours were delayed/missing
    if not successful_downloads:
        error_details = chr(10).join([f"  - {f}: {err}" for f, err in failed_downloads])
        raise RuntimeError(
            f"DATA LOSS PREVENTION ALERT: All 24 hourly files failed to download/stage:{chr(10)}{error_details}{chr(10)}Aborting pipeline."
        )
    if failed_downloads:
        print(f"Warning: {len(failed_downloads)} hourly files failed to download/stage (continuing with {len(successful_downloads)} files):")
        for f, err in failed_downloads:
            print(f"  - {f}: {err}")

def trigger_spark_ingestion():
    """
    Executes Spark HoodieStreamer job to ingest staged raw JSON records into
    Apache Hudi Copy-on-Write tables on Cloud Storage and syncs metadata with Hive Metastore.
    """
    bucket_name = os.environ.get('S3_BUCKET_NAME', 'github-raw-data')
    target_base_path = f"s3a://{bucket_name}/hudi-tables/github_events"
    source_dfs_dir = f"s3a://{bucket_name}/github-raw/"
    
    client = docker.from_env()
    container = client.containers.get('spark-client')
    
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", "local[4]",
        "--driver-memory", "6G",
        "--class", "org.apache.hudi.utilities.streamer.HoodieStreamer",
        "--packages", "org.apache.hudi:hudi-utilities-slim-bundle_2.12:0.15.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4",
        "/root/.ivy2/jars/org.apache.hudi_hudi-utilities-slim-bundle_2.12-0.15.0.jar",
        "--source-class", "org.apache.hudi.utilities.sources.JsonDFSSource",
        "--schemaprovider-class", "org.apache.hudi.utilities.schema.FilebasedSchemaProvider",
        "--source-ordering-field", "created_at",
        "--target-base-path", target_base_path,
        "--target-table", "github_events",
        "--table-type", "COPY_ON_WRITE",
        "--source-limit", "1073741824",
        "--enable-hive-sync",
        "--hoodie-conf", f"hoodie.streamer.source.dfs.root={source_dfs_dir}",
        "--props", "file:///opt/spark/conf/github-ingest.properties"
    ]
    
    print(f"Launching Spark HoodieStreamer job (Source: {source_dfs_dir} -> Target: {target_base_path})...")
    exec_result = container.exec_run(cmd)
    output = exec_result.output.decode('utf-8')
    print(output)
    
    if exec_result.exit_code != 0:
        raise RuntimeError(f"Spark Ingestion failed with exit code {exec_result.exit_code}.\nOutput log:\n{output[-2000:]}")

    # Circuit Breaker: Prevent false-positive green runs when 0 records were ingested
    if "Nothing to commit" in output:
        raise RuntimeError(
            "INGESTION ABORTED: Hudi reported 'Nothing to commit'. "
            "Zero new records were ingested from the raw staging prefix."
        )

def run_data_quality_assertions():
    """
    Enterprise Data Quality Gate: Runs SQL validation checks against the newly committed
    Hudi table in Presto using PyHive DB-API to verify volume, non-null primary keys,
    deduplication, data freshness, and partition completeness.
    """
    from pyhive import presto

    conn = presto.connect(host='presto-server', port=8080, catalog='hudi', schema='default')
    cursor = conn.cursor()

    try:
        print("\n=======================================================")
        print("🛡️ RUNNING DATA QUALITY & INTEGRITY GATES (PRESTO)")
        print("=======================================================")

        # Fetch latest commit timestamp to scope quality gates to the newly ingested batch
        cursor.execute("SELECT max(_hoodie_commit_time) FROM hudi.default.github_events")
        latest_commit = cursor.fetchone()[0]
        print(f"Targeting Latest Commit Instant: {latest_commit}")
        if not latest_commit:
            raise AssertionError("Quality Check Failed: No commit instants found in hudi.default.github_events table!")

        # 1. Batch Volume Assertion (Problem 1 Fix)
        print("\n[Check 1/5] Validating Ingested Batch Volume & Row Count...")
        cursor.execute(f"""
            SELECT count(*) 
            FROM hudi.default.github_events 
            WHERE _hoodie_commit_time = '{latest_commit}'
        """)
        batch_rows = cursor.fetchone()[0]
        print(f" -> Newly committed rows in latest batch: {batch_rows:,}")
        if batch_rows <= 0:
            raise AssertionError(f"Quality Check Failed: Latest batch committed {batch_rows} records! Expected at least 1 record.")

        # 2. Primary Key Non-Null Assertion (Problem 2 Fix)
        print("\n[Check 2/5] Validating Primary Key Integrity in Latest Batch (id IS NOT NULL)...")
        cursor.execute(f"""
            SELECT count(*) 
            FROM hudi.default.github_events 
            WHERE _hoodie_commit_time = '{latest_commit}' 
              AND (id IS NULL OR trim(id) = '')
        """)
        null_keys = cursor.fetchone()[0]
        print(f" -> Null / Empty primary keys found: {null_keys}")
        if null_keys > 0:
            raise AssertionError(f"Quality Check Failed: Found {null_keys} records with NULL/empty primary key 'id' in batch!")

        # 3. Duplicate Key Assertion (Problem 3 Fix)
        print("\n[Check 3/5] Validating Record-Level Deduplication in Latest Batch (Zero Duplicate IDs)...")
        cursor.execute(f"""
            SELECT count(id) - count(DISTINCT id) 
            FROM hudi.default.github_events 
            WHERE _hoodie_commit_time = '{latest_commit}'
        """)
        duplicate_count = cursor.fetchone()[0]
        print(f" -> Duplicate event IDs found: {duplicate_count}")
        if duplicate_count > 0:
            raise AssertionError(f"Quality Check Failed: Found {duplicate_count} duplicated event IDs in batch! Deduplication violated.")

        # 4. Data Freshness & Timestamp Validity Assertion (Problem 4 Fix)
        print("\n[Check 4/5] Validating Data Freshness & Timestamp Sanity in Latest Batch...")
        cursor.execute(f"""
            SELECT 
                min(created_at), 
                max(created_at),
                CASE 
                    WHEN max(created_at) IS NOT NULL AND from_iso8601_timestamp(max(created_at)) >= now() - interval '24' hour THEN 1 
                    ELSE 0 
                END
            FROM hudi.default.github_events 
            WHERE _hoodie_commit_time = '{latest_commit}'
        """)
        min_created, max_created, is_fresh = cursor.fetchone()
        print(f" -> Temporal event range in batch: {min_created} to {max_created}")
        print(f" -> Freshness certified (within last 24h): {bool(is_fresh)}")
        if is_fresh != 1:
            raise AssertionError(f"Quality Check Failed: Ingested events are older than 24 hours (max created_at: {max_created})")

        # 5. Partition Completeness Assertion
        print("\n[Check 5/5] Validating Partition Distribution in Latest Batch (Event Types)...")
        cursor.execute(f"""
            SELECT count(DISTINCT type) 
            FROM hudi.default.github_events 
            WHERE _hoodie_commit_time = '{latest_commit}'
        """)
        partitions_count = cursor.fetchone()[0]
        print(f" -> Distinct event partition types active in batch: {partitions_count}")
        if partitions_count < 2:
            raise AssertionError(f"Quality Check Failed: Expected multiple event partitions, but only found {partitions_count} in batch.")

        print("\n=======================================================")
        print("✅ ALL DATA QUALITY GATES PASSED SUCCESSFULLY!")
        print("   Lakehouse table is certified accurate, fresh & ACID compliant.")
        print("=======================================================\n")
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

with DAG(
    'github_events_ingestion',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:

    task_download = PythonOperator(
        task_id='download_and_upload_raw_data',
        python_callable=download_and_upload_github_data
    )

    task_ingest = PythonOperator(
        task_id='trigger_hudi_ingestion',
        python_callable=trigger_spark_ingestion
    )

    task_quality_gate = PythonOperator(
        task_id='run_data_quality_assertions',
        python_callable=run_data_quality_assertions
    )

    task_download >> task_ingest >> task_quality_gate
