
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark import Row

def set_neg_to_zero(df, col_names):
    for col_name in col_names:
            df = df.withColumn(col_name, F.when(df[col_name]<0, F.lit(0))\
            .otherwise(df[col_name]))
    return df

def set_miss_to_zero(df, col_names):
    for col_name in col_names:
        df = df.withColumn(col_name, F.when(df[col_name].isNull(), F.lit(0))\
        .otherwise(df[col_name]))
    return df