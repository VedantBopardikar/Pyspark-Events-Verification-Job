# Documentation for PySpark Events Verification Job

This documentation provides an overview of the PySpark script designed for verifying events, reading event catalogs, and generating benchmarking results. The script leverages various PySpark functions to process data stored in Parquet and CSV formats and outputs a detailed analysis in CSV format.

## Project Overview

The goal of this script is to:

- Extract and process event data from an event catalog stored in Parquet format.
- Compare the extracted data with defined events in a CSV file.
- Calculate the completeness of the event data and benchmark it against defined standards.
- Output the final results in CSV format for further analysis.

## PySpark Functions Used

1. **SparkSession**
   - **Description:** The entry point to programming Spark with the DataFrame API. It’s used to create a session that enables the execution of PySpark jobs.
   - **Usage:**
     ```python
     spark = SparkSession.builder.appName("Events Verification").getOrCreate()
     ```

2. **read.parquet()**
   - **Description:** Reads a Parquet file into a DataFrame.
   - **Usage:**
     ```python
     event_catalog_df = spark.read.parquet(f"s3://path/to/parquet")
     ```

3. **read.csv()**
   - **Description:** Reads a CSV file into a DataFrame.
   - **Usage:**
     ```python
     defined_events_df = spark.read.option("header", "true").option("inferSchema", "true").csv("s3://path/to/csv")
     ```

4. **repartition()**
   - **Description:** Increases or decreases the number of partitions in a DataFrame.
   - **Usage:**
     ```python
     event_catalog_df = event_catalog_df.repartition(8)
     ```

5. **cache()**
   - **Description:** Persists the DataFrame in memory for faster access in subsequent operations.
   - **Usage:**
     ```python
     event_catalog_df.cache()
     ```

6. **distinct()**
   - **Description:** Returns a new DataFrame with distinct rows based on all or specific columns.
   - **Usage:**
     ```python
     unique_channels = [row["channel"] for row in defined_events_df.select("channel").distinct().collect()]
     ```

7. **filter()**
   - **Description:** Filters rows that satisfy a given condition.
   - **Usage:**
     ```python
     channel_defined_events_df = defined_events_df.filter(defined_events_df.channel == channel)
     ```

8. **withColumn()**
   - **Description:** Adds or replaces a column in a DataFrame.
   - **Usage:**
     ```python
     channel_event_catalog_df = channel_event_catalog_df.withColumn("extracted_version", get_json_object(col('context'), '$.app.version'))
     ```

9. **groupBy()**
   - **Description:** Groups the DataFrame using the specified columns, and performs aggregate functions on the grouped data.
   - **Usage:**
     ```python
     total_records_in_each_event = channel_event_catalog_df.groupBy("event_name").count()
     ```

10. **agg()**
    - **Description:** Computes aggregate statistics for grouped data.
    - **Usage:**
      ```python
      result_df = pivoted_completeness_df.groupBy("prop_name").agg(collect_list("value")).alias("values")
      ```

11. **explode()**
    - **Description:** Returns a new row for each element in the given array or map column.
    - **Usage:**
      ```python
      context_keys = channel_event_catalog_df.select(explode(json_object_keys("context")).alias("key"))
      ```

12. **get_json_object()**
    - **Description:** Extracts JSON elements from a string column containing JSON data.
    - **Usage:**
      ```python
      col("context").rlike("|".join(org_filtered_keys_list))
      ```

13. **join()**
    - **Description:** Joins two DataFrames based on a given condition.
    - **Usage:**
      ```python
      final_key_results = key_result_df.join(keys_result, (key_result_df.prop_name == keys_result.exploded_key) & (key_result_df.event_name == keys_result.event_name), 'left')
      ```

14. **selectExpr()**
    - **Description:** Selects a group of columns using SQL expressions.
    - **Usage:**
      ```python
      pivoted_completeness_df = completeness_data.selectExpr(f"stack({final_stack_string}) as (prop_name, value)")
      ```

15. **monotonically_increasing_id()**
    - **Description:** Generates monotonically increasing 64-bit integers.
    - **Usage:**
      ```python
      completeness_data = completeness_data.withColumn("row_id", monotonically_increasing_id())
      ```

16. **size()**
    - **Description:** Returns the length of an array or map.
    - **Usage:**
      ```python
      result_df = result_df.withColumn("value_null_count", completeness_data.count() - size("compact_value"))
      ```

17. **union()**
    - **Description:** Combines two DataFrames into one, preserving the rows of both.
    - **Usage:**
      ```python
      value_results_combined = value_results_combined.union(value_summery)
      ```

18. **coalesce()**
    - **Description:** Returns the first non-null value in the specified columns.
    - **Usage:**
      ```python
      coalesce(nullif(get_json_object(col('context'), '$.traits.project_id'), lit("")), nullif(get_json_object(col('properties'), '$.project_id'), lit("")))
      ```

19. **nullif()**
    - **Description:** Returns null if the first argument equals the second argument; otherwise, returns the first argument.
    - **Usage:**
      ```python
      coalesce(nullif(get_json_object(col('context'), '$.traits.project_id'), lit("")), nullif(get_json_object(col('properties'), '$.project_id'), lit("")))
      ```

20. **array_distinct()**
    - **Description:** Removes duplicate values from an array.
    - **Usage:**
      ```python
      event_records = event_records.withColumn("properties_keys", array_distinct(json_object_keys(channel_event_catalog_df.properties)))
      ```

21. **array_compact()**
    - **Description:** Removes nulls and empty strings from an array.
    - **Usage:**
      ```python
      result_df = result_df.withColumn("compact_value", array_compact(result_df["collect_list(value)"]))
      ```

22. **drop()**
    - **Description:** Drops specified columns from a DataFrame.
    - **Usage:**
      ```python
      completeness_data = completeness_data.drop(col("context"))
      ```

23. **lit()**
    - **Description:** Creates a new column with a constant value.
    - **Usage:**
      ```python
      result_df = result_df.withColumn("total_records",lit(completeness_data.count()))
      ```

24. **rank()**
    - **Description:** Assigns a rank to each row within a partition of a DataFrame, with ties getting the same rank.
    - **Usage:**
      ```python
      channel_defined_events_df = channel_defined_events_df.withColumn("rank", F.rank().over(window_spec))
      ```

25. **row_number()**
    - **Description:** Assigns a unique row number to each row within a window partition.
    - **Usage:**
      ```python
      eventrecords_df = event_records_properties_contexts_unnested.withColumn("row_id", row_number().over(windowSpec))
      ```

## Script Logic

1. **Initialize SparkSession:**
   - A Spark session is created to enable the execution of the PySpark job.

2. **Read Event Catalog and Defined Events:**
   - The event catalog is read from a Parquet file, and the defined events are read from a CSV file. The data is cached for faster processing.

3. **Filter and Rank Defined Events by Channel:**
   - The defined events are filtered by channel, and the latest version for each channel is selected using the `rank()` function.

4. **Filter Event Catalog by Channel and Version:**
   - The event catalog is filtered by the current channel and version extracted from the JSON data.

5. **Extract and Analyze Keys:**
   - JSON keys are extracted from the event data and analyzed to identify missing or incorrectly recorded keys.

6. **Calculate Completeness:**
   - Completeness of the data is calculated by comparing the extracted keys with the defined keys for each event. The results are stored in a DataFrame.

7. **Value Analysis:**
   - Values associated with each key are analyzed, and metrics such as null value count and non-null value count are calculated.

8. **Combine Results:**
   - The key analysis and value analysis results are combined to produce the final benchmarking data.

9. **Output Final Results:**
   - The final results are written to a CSV file for further analysis.

10. **Stop Spark Session:**
    - The Spark session is stopped to release resources.

## Conclusion

This PySpark job efficiently verifies events by reading event catalogs and benchmarking them against defined standards. The use of various PySpark functions enables complex data processing and analysis, leading to insightful results that can be further analyzed or visualized. This project is a valuable addition to your GitHub portfolio, showcasing your expertise in PySpark and data engineering.
