import pandas as pd
from collections import OrderedDict
import numbers
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession

def missing_df(df, spark_session=None):
    '''
    
    This function counts the number of missing on each variable in a spark dataframe and returns the result as a pandas dataframe.
     
    :param df: The spark dataframe for which to run missing count.
    :param spark_session: defines the Spark session to be used by the function. Default is None, which
                        means the function tries to catch the current session.
    :type df: dataframe
    :type spark_session: string
    
    Returns: a pandas dataframe
    Dataframe: A pandas dataframe with one row for each variable in the original dataset and the share of missing on each variable.
    
    '''
    
    #Parameter df --> datasett som det skal kjøres opptelling av missing for
    if (isinstance(df, DataFrame)):
        
        if spark_session is None:
            spark = SparkSession.builder.getOrCreate()            
        else:
            spark = spark_session
        
        #Transformerer spark dataframe til pandas dataframe for å kunne benytte pandas funksjoner
        df = df.toPandas()

        #Omformer datasettet til å inneholde false for celler som ikke har missing verdi og true for celler som har missing verdi
        df = pd.isna(df)

        #Teller opp boolske verdier for alle variabler, setter de som får missing på opptelling til 0
        # Dette skjer hvis de ikke har noen missing verdier på variabelen eller at alle er missing
        df = df.apply(pd.value_counts).fillna(0)

        #Kjører transpose slik at alle variabler er en egen record
        df = df.transpose()

        #Etter transpose er variablene på dataframen bolske verdier, True for antall missing, False for antall ikke missing
        #Gir de derfor missing nytt navn, sletter ikke missing og beregner andel missing av totalen
        if 1 in list(df.columns):
            df.rename(columns={1:"missing"}, inplace=True)
        else:
            df['missing'] = 0
        if 0 in list(df.columns):
            df['shareoftotal'] = df['missing']/(df['missing'] + df[0])        
            df.drop(columns=[0], inplace=True)
        else:
            df['shareoftotal'] = 1


        return df
    else:
        raise Exception('Parameter df må være en dataframe')
        return

def spark_qual_missing(df, df_name='', spark_session=None):
    
    '''
    
    This function counts the number of missing on each variable in a spark dataframe and returns the result as a spark dataframe.
     
    :param df: The spark dataframe for which to run missing count.
    :type df: dataframe
    :param df_name: Optional. Name of dataframe added as a column. Default is none.
    :type df_name: string
    :param spark_session: defines the Spark session to be used by the function. Default is None, which
                        means the function tries to catch the current session.
    :type spark_session: string
    
    Returns: a spark dataframe
    Dataframe: A spark dataframe with one row for each variable in the original dataset and the share of missing on each variable.
    
    '''
    
    if spark_session is None:
        spark = SparkSession.builder.getOrCreate()            
    else:
        spark = spark_session
    
    if (isinstance(df, DataFrame)):
        df_count = df.select([F.count(F.when(F.isnull(c), c)).alias(c) for c in df.columns])
        count_tot = df.count()
        df_log = []
            
        if len(df_name)>0:
            missing_variabler = [StructField('dataframe', StringType(), False),\
                   StructField('variable', StringType(), True),\
                   StructField('datatype', StringType(), True),\
                   StructField('obs_total', IntegerType(), False),\
                   StructField('obs_missing', IntegerType(), False),\
                   StructField('percentage_missing', FloatType(), False)]
            missing_schema = StructType(missing_variabler)
            
            for row in df_count.rdd.collect():
                for k in df.schema.fields:
                    df_row = {}
                    
                    df_row['dataframe']= df_name
                    df_row['variable'] = k.name
                    df_row['datatype'] =str(k.dataType)
                    df_row['obs_total'] = count_tot
                    df_row['obs_missing'] = row[k.name]
                    df_row['percentage_missing'] = (row[k.name]/count_tot)*100 
                    
                    df_log.append(df_row)

        else:
            missing_variabler = [StructField('variable', StringType(), True),\
                                 StructField('datatype', StringType(), True),\
                                 StructField('obs_total', IntegerType(), False),\
                                 StructField('obs_missing', IntegerType(), False),\
                                 StructField('percentage_missing', FloatType(), False)]
            missing_schema = StructType(missing_variabler)
           
            for row in df_count.rdd.collect():
                for k in df.schema.fields:
                    df_row = {}
                    
                    df_row['variable'] = k.name
                    df_row['datatype'] =str(k.dataType)
                    df_row['obs_total'] = count_tot
                    df_row['obs_missing'] = row[k.name]
                    df_row['percentage_missing'] = (row[k.name]/count_tot)*100
                    
                    df_log.append(df_row)
        
        if len(df_log)>0:
            rdd_missing = spark.sparkContext.parallelize(df_log)
            df_missing = spark.createDataFrame(rdd_missing, missing_schema)
        else:
            df_missing = spark.createDataFrame(spark.sparkContext.emptyRDD(), missing_schema)
        
        return df_missing
    else:
        raise Exception('Parameter df må være en dataframe')
        return    
    