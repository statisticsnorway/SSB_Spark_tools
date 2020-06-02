from pyspark.sql import DataFrame
from pyspark import Row
from pyspark.sql.types import *
from itertools import chain
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext

#def listcode_check(df, variabel, kodeliste, sqlContext = SQLContext(sc)):
def listcode_check(df, variabel, kodeliste):
    
    if (isinstance(df, DataFrame)) & (isinstance(variabel, str)) & (isinstance(kodeliste, type([]))):
        sc = SparkContext.getOrCreate()
        sqlContext = SQLContext(sc)
        
        sjekk_listedf = []
        sjekk_bol = True
        koder_df = df.groupby(variabel).count().withColumnRenamed('count', 'antall')
        for row in koder_df.rdd.collect():
            dRow = {}
            dRow['kode'] = row[variabel]
            dRow['antall'] = row['antall']
            if row[variabel] in kodeliste:
                dRow['i_kodeliste'] = True
            else:
                dRow['i_kodeliste'] = False
                sjekk_bol = False
            sjekk_listedf.append(dRow)
            
        field_kl = [StructField('kode', StringType(), False),\
                        StructField('antall', IntegerType(), True),\
                        StructField('i_kodeliste', BooleanType(), False)]
        
        schema_kl = StructType(field_kl)
        rdd_sl = sc.parallelize(sjekk_listedf)
        sjekk_df = sqlContext.createDataFrame(rdd_sl, schema_kl)
    
        return sjekk_bol, sjekk_df
    else:
        if not (isinstance(df, DataFrame)):
            raise Exception('Første parameter må være en dataframe som har variabelen som skal sjekkes')
            return
        if not (isinstance(variabel, str)):
            raise Exception('Andre parameter må være en string med navnet på variabel som skal sjekkes')
            return
        
        if not (isinstance(kodeliste, type([]))):
            raise Exception('Tredje parameter må være en python liste som inneholder kodelisten variabel skal sjekkes mot')
            return
            
def listcode_lookup(df, variabel, kodeliste, nokkelverdi):
    kodeliste_dict = {}
    
    for row in kodeliste.rdd.collect():
        kodeliste_dict[row[nokkelverdi[0]]] = row[nokkelverdi[1]]
    
    mapping_expr = F.create_map([F.lit(x) for x in chain(*kodeliste_dict.items())])
    df = df.withColumn("{}_kodelisteverdi".format(variabel), mapping_expr.getItem(F.col(variabel))) 
    return df
