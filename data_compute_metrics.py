""" Compute metrics from data such as
- total count
- distinct count of columns separated by ','
- count of true values of columns separaetd by ','
- group by one column (could extend to multiple columns)
 - min/avg/max values of numerical columns (separated by ',')
 - count of non-zero values of columns (separated by ',')
 - count of non-null values of columns (separated by ',')
- record count of a custom SQL filter
Author: Kemin Li
"""
import sys
import s3fs
import pandas as pd
import yaml
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import pyspark.sql.functions as F

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

args = getResolvedOptions(
  sys.argv,
  ['input_path', 'output_path', 'enable_cache', 'metrics_definition_path']
)
input_path = args["input_path"] # s3://cc-dataeng-staging/fraudlogix/eilv4
output_path = args["output_path"].strip('/') # s3://cc-dataeng-staging/fraudlogix/eilv4
enable_cache = args["enable_cache"]
metrics_definition_path = args["metrics_definition_path"]

# read metrics definition file
s3 = s3fs.S3FileSystem(anon=False)
with s3.open(metrics_definition_path) as f:
    metric_definitions = yaml.safe_load(f)['metric_definitions']

print("processing", input_path)
df = spark.read.parquet(input_path)
if enable_cache.lower() == "true":
    df.persist()

df_list = []
# loop over all metrics type
for metric, arguments in metric_definitions.items():
    # count_notnull_cols: ['*', 'id']
    if metric == 'count_notnull_cols' and isinstance(arguments, list):
        print(metric, arguments)
        for col in arguments:
            print(f"count {col}")
            if col == '*':
                df_list.append(
                    df.agg(
                        F.first(F.lit('count')).alias('metric'),
                        F.count(col).alias('value')
                    ).toPandas()
                )
            else:
                df_list.append(
                    df.agg(
                        F.first(F.lit(f'{col}_notnull')).alias('metric'),
                        F.count(col).alias('value')
                    ).toPandas()
                )
    elif metric == 'count_notnull_cols':
        print(f"{metric} argument type must be list, got {type(arguments)}.")

    # count_distinct_cols: ['id']
    if metric == 'count_distinct_cols' and isinstance(arguments, list):
        print(metric, arguments)
        for col in arguments:
            print(f"distinct count of {col}")
            df_list.append(
                df.agg(
                    F.first(F.lit(f'distinct_{col}')).alias('metric'),
                    F.countDistinct(col).alias('value')
                ).toPandas()
            )
    elif metric == 'count_distinct_cols':
        print(f"{metric} argument type must be list, got {type(arguments)}.")

    # count_true_cols: [pci, phi, pii]
    if metric == 'count_true_cols' and isinstance(arguments, list):
        print(metric, arguments)
        for col in arguments:
            print(f"true count of {col}")
            df_list.append(
                df.agg(
                    F.first(F.lit(f'count_true_{col}')).alias('metric'),
                    F.sum(F.col(col).cast('integer')).alias('value')
                ).toPandas()
            )
    elif metric == 'count_true_cols':
        print(f"{metric} argument type must be list, got {type(arguments)}.")

    # numerical_cols: [age, height]
    if metric == 'numerical_cols' and isinstance(arguments, list):
        print(metric, arguments)
        for col in arguments:
            print(f'min({col})')
            df_list.append(
                df.agg(
                    F.first(F.lit(f'{col}_min')).alias('metric'),
                    F.min(col).alias('value')
                ).toPandas()
            )
            print(f'mean({col})')
            df_list.append(
                df.agg(
                    F.first(F.lit(f'{col}_avg')).alias('metric'),
                    F.mean(col).alias('value')
                ).toPandas()
            )
            print(f'max({col})')
            df_list.append(
                df.agg(
                    F.first(F.lit(f'{col}_max')).alias('metric'),
                    F.max(col).alias('value')
                ).toPandas()
            )
    elif metric == 'numerical_cols':
        print(f"{metric} argument type must be list, got {type(arguments)}.")

    # aggregations
    #  - groupby_col: gender
    #  - agg_numerical_cols: [age, height]
    #  - agg_notnull_count_cols: [address, phone]
    #  - agg_nonzero_count_cols: null
    if metric == 'aggregations' and isinstance(arguments, dict):
        if 'groupby_cols' in arguments and isinstance(arguments['groupby_cols'], list):
            groupby_cols = arguments['groupby_cols']
            if 'agg_numerical_cols' in arguments and isinstance(arguments['agg_numerical_cols'], list):
                for col in arguments['agg_numerical_cols']:
                    print(f'min({col}) group by {groupby_cols}')
                    df_list.append(
                        df
                        .groupBy(groupby_cols)
                        .agg(F.min(col).alias('value'))
                        .withColumn('metric', F.regexp_replace(F.concat_ws('_', *groupby_cols, F.lit(f'{col}_min')), ' ', ''))
                        .drop(*groupby_cols)
                        .toPandas()
                    )
                    print(f'mean({col}) group by {groupby_cols}')
                    df_list.append(
                        df
                        .groupBy(groupby_cols)
                        .agg(F.mean(col).alias('value'))
                        .withColumn('metric', F.regexp_replace(F.concat_ws('_', *groupby_cols, F.lit(f'{col}_avg')), ' ', ''))
                        .drop(*groupby_cols)
                        .toPandas()
                    )
                    print(f'max({col}) group by {groupby_cols}')
                    df_list.append(
                        df
                        .groupBy(groupby_cols)
                        .agg(F.max(col).alias('value'))
                        .withColumn('metric', F.regexp_replace(F.concat_ws('_', *groupby_cols, F.lit(f'{col}_max')), ' ', ''))
                        .drop(*groupby_cols)
                        .toPandas()
                    )
            elif 'agg_numerical_cols' in arguments:
                print(f"agg_numerical_cols argument type must be list, got {type(arguments['agg_numerical_cols'])}.")

            if 'agg_nonzero_count_cols' in arguments and isinstance(arguments['agg_nonzero_count_cols'], list):
                for col in arguments['agg_nonzero_count_cols']:
                    print(f'nonzero count of {col} group by {groupby_cols}')
                    df_list.append(
                        df
                        .groupBy(groupby_cols)
                        .agg(F.sum((F.col(col)!=0.0).astype('int')).alias('value'))
                        .withColumn('metric', F.regexp_replace(F.concat_ws('_', *groupby_cols, F.lit(f'{col}_nonzero')), ' ', ''))
                        .drop(*groupby_cols)
                        .toPandas()
                    )
            elif 'agg_nonzero_count_cols' in arguments:
                print(f"agg_nonzero_count_cols argument type must be list, got {type(arguments['agg_nonzero_count_cols'])}.")

            if 'agg_notnull_count_cols' in arguments and isinstance(arguments['agg_notnull_count_cols'], list):
                for col in arguments['agg_notnull_count_cols']:
                    print(f'notnull count of {col} group by {groupby_cols}')
                    df_list.append(
                        df
                        .groupBy(groupby_cols)
                        .agg(F.count(col).alias('value'))
                        .withColumn('metric', F.regexp_replace(F.concat_ws('_', *groupby_cols, F.lit(f'{col}_notnull')), ' ', ''))
                        .drop(*groupby_cols)
                        .toPandas()
                    )
            elif 'agg_notnull_count_cols' in arguments:
                print(f"agg_notnull_count_cols argument type must be list, got {type(arguments['agg_notnull_count_cols'])}.")
        elif 'groupby_cols' in arguments:
            print(f"groupby_cols argument type must be list, got {type(arguments['groupby_cols'])}.")

    elif metric == 'aggregations':
        print(f"{metric} argument type must be dict, got {type(arguments)}.")

    # custom_counts: [[column_name, query]]
    # ["number_text_conflict_count", "number_value=0 AND text_value IS NOT NULL AND text_value!=''"]
    if metric == 'custom_counts' and isinstance(arguments, list):
        print(metric, arguments)
        for column_query in arguments:
            if len(column_query) == 2:
                column, query = column_query
                print(f"count by filter: {query}")
                df_list.append(
                    pd.DataFrame(
                        [[column, df.filter(query).count()]],
                        columns=['metric', 'value']
                    )
                )
            else:
                print(f"{column_query} must have size 2, got {len(column_query)}")
    elif metric == 'custom_counts':
        print(f"{metric} argument type must be list, got {type(arguments)}.")

if enable_cache.lower() == "true":
    df.unpersist()

df_out = pd.concat(df_list)
print(df_out.shape)
out_file = f"{output_path}/metrics.parquet"
df_out.set_index('metric').T.to_parquet(out_file, index=False)
