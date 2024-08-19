from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, get_json_object, explode, when, lit, size, array, concat,
    monotonically_increasing_id, collect_list, coalesce, nullif, array_distinct,
     array_compact, json_object_keys, row_number, rank)
from pyspark.sql.types import MapType, StringType, ArrayType,StructField,StructType
from pyspark.sql import functions as F

from pyspark.sql.window import Window



spark = SparkSession.builder \
    .appName("Events Verification") \
    .getOrCreate()

#read event catalog
process_date = "2024-06-01"
event_date = "2024-06-01"

event_catalog_df = spark.read.parquet(f"s3://powerplay-product-analytics/product-analytics-datalake/event-catalog/process_date={process_date}/event_date={event_date}")
#event_catalog_df = spark.read.csv("/Users/powerplay/Downloads/part-00000-63951172-5584-4482-88c6-d7b03e6e6127-c000.csv", header=True, inferSchema=True)
event_catalog_df = event_catalog_df.repartition(8)
event_catalog_df.cache()
# event_catalog_df.cache()
#read defined events
defined_events_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3://powerplay-business-analytics/de-adhoc-analytics/vedant-analytics/events_verification/Release Tracker_ Events Benchmarking - event-catalog.csv")
print("count of defined_events_df", defined_events_df.count())

unique_channels = [row["channel"] for row in defined_events_df.select("channel").distinct().collect()]
results = []
for channel in unique_channels:
    window_spec = Window.partitionBy("channel").orderBy(F.desc("version"))
    channel_defined_events_df = defined_events_df.filter(defined_events_df.channel == channel)
    channel_defined_events_df = channel_defined_events_df.withColumn("rank", F.rank().over(window_spec))
    current_version = channel_defined_events_df.filter(channel_defined_events_df.rank == 1).select("version").first()[0]
    print(current_version)
    channel_defined_events_df = channel_defined_events_df.filter((channel_defined_events_df.channel==channel) & (channel_defined_events_df.version == current_version))
    channel_event_catalog_df = event_catalog_df.withColumn("extracted_version", get_json_object(col('context'), '$.app.version'))
    channel_event_catalog_df = channel_event_catalog_df.filter((channel_event_catalog_df.client_name == channel) & (channel_event_catalog_df.extracted_version==str(current_version)))
    total_records_in_each_event = channel_event_catalog_df.groupBy("event_name").count()
    total_records_in_each_event = total_records_in_each_event.withColumnRenamed("count", "total_records")
    #total_records_in_each_event.show()
    #keys analytics derivation
    org_id_identifiers = ['organisation_id', 'ord_id', 'org_id', 'orgId', 'org_id']
    project_id_identifiers = ['project_id']
    # Extract keys from JSON columns and explode arrays into individual rows
    context_keys = channel_event_catalog_df.select(explode(json_object_keys("context")).alias("key"))
    traits_keys = channel_event_catalog_df.select(explode(json_object_keys("traits")).alias("key"))
    properties_keys = channel_event_catalog_df.select(explode(json_object_keys("properties")).alias("key"))
    nested_traits_keys = channel_event_catalog_df.withColumn(
        "context_traits_keys",
        json_object_keys(get_json_object(col("context"), "$.traits"))
    ).select(explode(col("context_traits_keys")).alias("key"))
    nested_metadata_keys = channel_event_catalog_df.withColumn(
        "properties_meta_data_keys",
        json_object_keys(get_json_object(col("properties"), "$.meta_data"))
    ).select(explode(col("properties_meta_data_keys")).alias("key"))
    all_keys = context_keys.union(traits_keys).union(properties_keys).union(nested_traits_keys).union(nested_metadata_keys).distinct()
    org_filtered_keys = all_keys.filter(col("key").isin(org_id_identifiers))
    proj_filtered_keys = all_keys.filter(col("key").isin(project_id_identifiers))
    org_filtered_keys_list = [row.key for row in org_filtered_keys.collect()]
    proj_filtered_keys_list = [row.key for row in proj_filtered_keys.collect()]
    org_id_key_present = channel_event_catalog_df.withColumn(
        "org_id_key_present",
        col("context").rlike("|".join(org_filtered_keys_list)) |
        col("traits").rlike("|".join(org_filtered_keys_list)) |
        col("properties").rlike("|".join(org_filtered_keys_list)))
    project_id_key_present = org_id_key_present.withColumn(
        "project_id_key_present",
        col("context").rlike("|".join(proj_filtered_keys_list)) |
        col("traits").rlike("|".join(proj_filtered_keys_list)) |
        col("properties").rlike("|".join(proj_filtered_keys_list)))
    event_records = project_id_key_present.withColumn(
        'org_id_key',
        when(col('org_id_key_present'), array(lit('org_id'))).otherwise(array())
    ).drop("org_id_key_present")
    event_records = event_records.withColumn(
        'project_id_key',
        when(col('project_id_key_present'), array(lit('project_id'))).otherwise(array())
    ).drop("project_id_key_present")
    event_records = event_records.withColumn("properties_keys", array_distinct(json_object_keys(channel_event_catalog_df.properties)))
    event_records = event_records.withColumn("context_keys", array_distinct(json_object_keys(channel_event_catalog_df.context)))

    ##event_records = event_records.withColumn("properties_context_merged_keys", expr("array_distinct(array_union(properties_keys,context_keys))"))
    #project_id_key_array,org_id_key_array,
    event_records_properties_contexts_unnested = event_records.withColumn(
        "merged_keys",
    array_distinct(concat(col("org_id_key"),col("project_id_key"),col("properties_keys"),col("context_keys")))
    )#.drop("org_id_proj_id_merged", "properties_context_merged_keys")

    windowSpec = Window.partitionBy("event_name").orderBy("event_name")
    eventrecords_df = event_records_properties_contexts_unnested.withColumn("row_id", row_number().over(windowSpec))
    #eventrecords_df.show()

    events = eventrecords_df.select("event_name","row_id", explode("merged_keys").alias("exploded_key"))
    keys_result = events.groupBy("exploded_key","event_name").count().orderBy("event_name")
    keys_result = keys_result.join(total_records_in_each_event, on="event_name")
    keys_result.show(truncate=False)
    channel_defined_events_df1 = channel_defined_events_df.drop("rank")
    channel_defined_events_df1 = channel_defined_events_df1.drop("release_date")

    defined_results = []
    for base_row in channel_defined_events_df1.collect(): 
        base_event_name = base_row["event_name"]
        for prop_name in base_row.asDict().values():
            if prop_name and prop_name != base_row["event_name"] and prop_name!="user_id" and prop_name!=channel and prop_name!=current_version and prop_name!=event_date and prop_name!=rank:
                defined_results.append((prop_name, base_event_name))
            
    result_schema = StructType([
        StructField("prop_name", StringType(), True),
        StructField("event_name", StringType(), True)
    ])

# Create the result DataFrame
    key_result_df = spark.createDataFrame(defined_results, schema=result_schema)
    final_key_results = key_result_df.join(
        keys_result,
        (key_result_df.prop_name == keys_result.exploded_key) & (key_result_df.event_name == keys_result.event_name),
        'left'
    ).select(
        key_result_df.prop_name,
        key_result_df.event_name,
        keys_result['count'].alias('keys_not_null_count'),
        keys_result.total_records
    )
    final_key_results.show(truncate=False)
    #final_key_result percentage calculation remaining of each property in a event
    #combining key analytics and value analytics
    completeness_dfs = {}
    for base_row in channel_defined_events_df1.collect(): 
        
        base_event_name = base_row["event_name"]
        print(base_event_name)
        event_records = channel_event_catalog_df.filter(col("event_name") == base_event_name)


        # part 1 of missing keys ends here
        completeness_data = event_records.select(
            #col("org_id"),
            #col("project_id"),
            col("user_id"),
            col("context"),
            col("properties"),
            col("traits")
        )
        print("count of completeness_data:", completeness_data.count())
    
        print(base_row)
        for key, prop_name in base_row.asDict().items():
            if prop_name and prop_name != base_row["event_name"] and prop_name!="user_id" and prop_name!=channel and prop_name!=current_version and prop_name!=event_date and prop_name!=rank:
                print(prop_name)
                if prop_name == "org_id":
                    completeness_data = completeness_data.withColumn(
                    prop_name,
                    coalesce(nullif(get_json_object(col('context'),'$.traits.organisation_id'), lit("")), nullif(get_json_object(col('traits'),'$.organisation_id'), lit("")), nullif(get_json_object(col('properties'),'$.organisation_id'), lit("")), nullif(get_json_object(col('traits'),'$.ord_id'), lit("")), nullif(get_json_object(col('properties'),'$.meta_data.org_id'), lit("")), nullif(get_json_object(col('properties'),'$.org_id'), lit("")), nullif(get_json_object(col('properties'),'$.orgId'), lit(""))).alias('org_id')
                )
                elif prop_name == "project_id":
                    completeness_data = completeness_data.withColumn(
                    prop_name,
                    coalesce(nullif(get_json_object(col('context'), '$.traits.project_id'), lit("")), nullif(get_json_object(col('properties'), '$.project_id'), lit("")), nullif(get_json_object(col('properties'), '$.meta_data.project_id'), lit("")), nullif(get_json_object(col('traits'), '$.project_id'), lit("")) ).alias('project_id')
                )
                else:
                    completeness_data = completeness_data.withColumn(
                    prop_name,
                    get_json_object(col("context"), f"$.{prop_name}"))

                    completeness_data = completeness_data.withColumn(
                    prop_name,
                    when(col(prop_name).isNull(), get_json_object(col("properties"), f"$.{prop_name}")).otherwise(
                        col(prop_name)))
                    
        completeness_data = completeness_data.drop(col("context"))
        completeness_data = completeness_data.drop(col("properties"))
        completeness_data = completeness_data.drop(col("traits"))
        completeness_dfs[base_row['event_name']] = completeness_data
                #completeness_data.head(1)
                
    value_results = []
    for key, value in completeness_dfs.items():
        print("key:", key)  # Print the key
        print("value:", value)  # Print the value
        prop_name_list = value.columns  
        completeness_data = value.withColumn("row_id", monotonically_increasing_id())
        prop_name_list = [str(item) for item in prop_name_list]
        print("prop_name_list :",prop_name_list) 
        completeness_data = completeness_data.select([col(x).cast("String") for x in prop_name_list])   
        properties = prop_name_list
        print("properties: ",properties)
        properties_count = len(properties) 
        print(properties_count)    
        stack_cols = ",".join([f"'{x}', {x}" for x in properties])   
        final_stack_string = f"{properties_count}, {stack_cols}"    
        print(final_stack_string)    
        pivoted_completeness_df = completeness_data.selectExpr(f"stack({final_stack_string}) as (prop_name, value)")    
        result_df = pivoted_completeness_df.groupBy("prop_name").agg(collect_list("value")).alias("values")
        result_df = result_df.withColumn("event_name",lit(key))
        result_df = result_df.withColumn("compact_value", array_compact(result_df["collect_list(value)"]))
        result_df = result_df.withColumn("value_null_count", completeness_data.count() - size("compact_value"))
        result_df = result_df.withColumn("value_not_null_count",size("compact_value"))
        result_df = result_df.drop(col("compact_value"))
        result_df = result_df.withColumn("value_null_count_percentage",col("value_null_count")*100/completeness_data.count())
        result_df = result_df.withColumn("total_records",lit(completeness_data.count()))
        
        result_df.show()
        value_results.append(result_df) 
        print("count = ",len(value_results))
            
    value_results_combined = value_results[0]
    for value_summery in value_results[1:]:
        value_results_combined = value_results_combined.union(value_summery)  
    channel_final_results = value_results_combined.join(final_key_results, (value_results_combined.event_name == final_key_results.event_name) & (value_results_combined.prop_name == final_key_results.prop_name), "left").select(value_results_combined.prop_name, value_results_combined.event_name, value_results_combined.value_null_count, value_results_combined.value_not_null_count, value_results_combined.value_null_count_percentage, final_key_results.keys_not_null_count, value_results_combined.total_records)
    channel_final_results = channel_final_results.withColumn("key_null_count",col("total_records") - col("keys_not_null_count"))
    channel_final_results = channel_final_results.withColumn("key_null_count_percentage",col("key_null_count")*100/col("total_records"))
    channel_final_results = channel_final_results.withColumn("release_date",lit(process_date))
    channel_final_results = channel_final_results.withColumn("channel",lit(channel))
    channel_final_results = channel_final_results.withColumn("version",lit(current_version))
    channel_final_results = channel_final_results.withColumn("event_date",lit(event_date))
    channel_final_results = channel_final_results.na.fill(value=0)
    channel_final_results.show()
    results.append(channel_final_results)
    print("lol")

final_results = results[0]
for r in results[1:]:
    final_results = final_results.union(r)
csv_output_path = "s3://powerplay-business-analytics/de-adhoc-analytics/vedant-analytics/events_verification/event_benchmarking.csv"
final_results.coalesce(1).write.csv(csv_output_path, header=True, mode="overwrite")

spark.stop()


