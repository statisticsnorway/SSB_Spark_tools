import pandas as pd
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

def missing_df(df, subsetvar=None):
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)
    
    df = df.toPandas()
    df = pd.isna(df)
    df = df.apply(pd.value_counts).fillna(0)
    df = df.transpose()
    
    if 1 in list(df.columns):
        df.rename(columns={1:"missing"}, inplace=True)
    else:
        df['missing'] = 0
    if 0 in list(df.columns):
        df['shareoftotal'] = df['missing']/(df['missing'] + df[0])        
        df.drop(columns=[0], inplace=True)
    else:
        df['shareoftotal'] = 1
    
    df['missing'].astype(int)
    df['variable'] = df.index
    
    df = df[['variable', 'missing', 'shareoftotal']]
    
    return spark.createDataFrame(df)
        