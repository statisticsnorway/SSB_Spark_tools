import pandas as pd
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

def missing_df(df, subsetvar=None):
    #Overfører sparkcontext til funksjon
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)
    
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
    
       
    #Setter index variabel som er listen av variabler fra orginal dataframe til egen variabel pga i transformasjon tilbake til spark dataframe
    #slettes index variabler
    df['variable'] = df.index
    
    #Endrer rekkefølgen på variablene til ønsket rekkefølge
    df = df[['variable', 'missing', 'shareoftotal']]
    
    #Transformer dataframe fra pandas til spark dataframe og returnerer resultatet
    return spark.createDataFrame(df)
        