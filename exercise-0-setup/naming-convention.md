# Naming convention

> [!TIP]
> Please review the naming conventions as that is crucial for all the exercises to run smoothly. **No action needed, just review and ack the naming convention**.

## Workspace name
Assign a name: `urban-innovation-deNNN`, where NNN represents the number assigned to you. For example, `urban-innovation-de001`.

## Table names
* Task 1.1.18 - `green201501`
* Task 1.3.7 - `green202301`

## Bronze Layer (Raw Data Management)
Lakehouse Name: `bronzerawdata`

This is the foundational layer where raw data is ingested directly from various sources, including yellow and green taxi trip records, FHV trip records, and potentially other urban mobility datasets. The data is stored in its original, unmodified form. In the context of your workshop, this involves landing raw TLC Trip Record Data into this layer, ensuring that all raw data remains immutable and traceable for lineage purposes.

## Silver Layer (Refined Data Management)
Lakehouse Name: `silvercleansed`

In this intermediate layer, data is cleansed, standardized, and enriched to resolve inconsistencies and prepare for more detailed analysis. This includes resolving issues with data quality, standardizing formats, and enriching taxi and FHV data with additional contextual information, such as weather conditions or traffic data. The goal here is to create a reliable, query-optimized dataset that supports more efficient analysis and reporting.

## Gold Layer (Curated Data Management)
Lakehouse Name: `goldcurated`

The highest level of the lakehouse, where data is further transformed, modeled, and summarized to support advanced analytics and business intelligence. This layer focuses on deriving actionable insights and supporting high-level decision-making. It could involve aggregating data into meaningful metrics, developing KPIs for urban transportation efficiency, or building machine learning models to predict future trends based on historical patterns.

## Lakehouse Schema (Public Preview)
Lakehouse supports the creation of custom schemas. Schemas allow you to group your tables together for better data discovery, access control, and more. To enable schema support for your lakehouse, check the box next to Lakehouse schemas (Public Preview) when you create it. Once you create the lakehouse, you can find a default schema named `dbo` under `Tables`. This schema is always there and can't be changed or removed. To create a new schema, hover over `Tables`, select …, and choose `New schema`. Enter your schema name and select `Create`. You'll see your schema listed under `Tables` in alphabetical order.

Bring multiple tables with schema shortcut - To reference multiple Delta tables from other Fabric lakehouse or external storage, use schema shortcut that displays all tables under the chosen schema or folder. Any changes to the tables in the source location also appear in the schema. To create a schema shortcut, hover over `Tables`, select on …, and choose `New schema shortcut`. Then select a schema on another lakehouse, or a folder with Delta tables on your external storage like Azure Data Lake Storage (ADLS) Gen2. That creates a new schema with your referenced tables.

You can move tables across these schema by dragging and dropping.