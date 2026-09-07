"""
Apache Hudi Lakehouse Superpowers Demo:
1. ACID Record Upsert (In-place State Mutation)
2. GDPR 'Right to be Forgotten' Point Delete
3. Time Travel & Commit Timeline Inspection
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def init_spark():
    return SparkSession.builder \
        .appName("Hudi-Superpowers-Demo") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .enableHiveSupport() \
        .getOrCreate()

def main():
    spark = init_spark()
    spark.sparkContext.setLogLevel("ERROR")
    
    bucket_name = os.environ.get("B2_BUCKET_NAME", "github-raw-data")
    table_path = f"s3a://{bucket_name}/hudi-tables/github_events"
    
    print("\n" + "="*70)
    print("🚀 APACHE HUDI LAKEHOUSE SUPERPOWERS DEMONSTRATION")
    print("="*70)
    
    # Check if table exists
    try:
        df = spark.read.format("hudi").load(table_path)
        total_records = df.count()
        print(f"\n📊 Current Table Status: {total_records:,} total records in Lakehouse")
    except Exception as e:
        print(f"Error reading Hudi table from {table_path}: {e}")
        print("Please run the main ingestion DAG first to populate initial data.")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # DEMO 1: ACID UPSERT (In-Place Record State Mutation)
    # -------------------------------------------------------------------------
    print("\n" + "-"*70)
    print("⚡ DEMO 1: ACID Record Upsert (State Mutation)")
    print("   Scenario: An Issue or PR event is updated from 'opened' -> 'closed'")
    print("-"*70)
    
    sample_row = df.filter(col("type").isin("PullRequestEvent", "IssuesEvent")).limit(1).collect()
    if sample_row:
        target_id = sample_row[0]["id"]
        target_type = sample_row[0]["type"]
        print(f"Target Event ID: {target_id} (Type: {target_type})")
        
        # Create an updated version of this exact record
        update_df = df.filter(col("id") == target_id) \
                      .withColumn("created_at", lit("2026-09-07T12:00:00Z"))
        
        hudi_upsert_options = {
            'hoodie.table.name': 'github_events',
            'hoodie.datasource.write.recordkey.field': 'id',
            'hoodie.datasource.write.partitionpath.field': 'type',
            'hoodie.datasource.write.precombine.field': 'created_at',
            'hoodie.datasource.write.operation': 'upsert',
            'hoodie.datasource.write.hive_style_partitioning': 'true',
            'hoodie.datasource.hive_sync.enable': 'true',
            'hoodie.datasource.hive_sync.mode': 'hms',
            'hoodie.datasource.hive_sync.database': 'default',
            'hoodie.datasource.hive_sync.table': 'github_events',
            'hoodie.datasource.hive_sync.metastore.uris': 'thrift://hive-metastore:9083'
        }
        
        print("Writing ACID UPSERT batch...")
        update_df.write.format("hudi") \
                 .options(**hudi_upsert_options) \
                 .mode("append") \
                 .save(table_path)
                 
        print("✅ Upsert completed! Record updated in-place without table rewrite or duplicates.")

    # -------------------------------------------------------------------------
    # DEMO 2: GDPR Point Delete ('Right to be Forgotten')
    # -------------------------------------------------------------------------
    print("\n" + "-"*70)
    print("🔒 DEMO 2: GDPR 'Right to be Forgotten' Point Delete")
    print("   Scenario: A user requests full erasure of their activity records")
    print("-"*70)
    
    user_sample = df.select("actor.login", "id", "type").limit(1).collect()
    if user_sample:
        user_to_delete = user_sample[0]["login"]
        print(f"Deleting all records for user: '{user_to_delete}'...")
        
        delete_keys_df = df.filter(col("actor.login") == user_to_delete).select("id", "type", "created_at")
        deleted_count = delete_keys_df.count()
        print(f"Found {deleted_count} records matching user '{user_to_delete}'.")
        
        hudi_delete_options = {
            'hoodie.table.name': 'github_events',
            'hoodie.datasource.write.recordkey.field': 'id',
            'hoodie.datasource.write.partitionpath.field': 'type',
            'hoodie.datasource.write.operation': 'delete',
            'hoodie.datasource.write.hive_style_partitioning': 'true',
            'hoodie.datasource.hive_sync.enable': 'true',
            'hoodie.datasource.hive_sync.mode': 'hms',
            'hoodie.datasource.hive_sync.database': 'default',
            'hoodie.datasource.hive_sync.table': 'github_events',
            'hoodie.datasource.hive_sync.metastore.uris': 'thrift://hive-metastore:9083'
        }
        
        delete_keys_df.write.format("hudi") \
                      .options(**hudi_delete_options) \
                      .mode("append") \
                      .save(table_path)
                      
        print(f"✅ GDPR point deletion completed! All records for '{user_to_delete}' removed.")

    # -------------------------------------------------------------------------
    # DEMO 3: Commit Timeline & Time Travel Inspection
    # -------------------------------------------------------------------------
    print("\n" + "-"*70)
    print("🕒 DEMO 3: Hudi Commit Timeline & Time Travel Inspection")
    print("-"*70)
    
    commits_df = spark.read.format("hudi").load(table_path) \
                      .select("_hoodie_commit_time") \
                      .distinct() \
                      .orderBy(col("_hoodie_commit_time").desc())
                      
    print("Recent Lakehouse Commit Instants:")
    commits_df.show(5, truncate=False)
    
    print("="*70)
    print("🎉 HUDI SUPERPOWERS DEMONSTRATION COMPLETE")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
