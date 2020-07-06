import pandas as pd
from collections import OrderedDict
import numbers
from pyspark.sql import DataFrame

def missing_df(df):
    '''
    
    This function counts the number of missing on each variable in a dataset.
     
    :param df: The dataframe for which to run missing count
    
    :type df: dataframe
    
    Returns: a dataframe
    Dataframe: A dataframe with one row for each variable in the original dataset and the share of missing on each variable.
    
    '''  
    
    #Parameter df --> datasett som det skal kjøres opptelling av missing for
    if (isinstance(df, DataFrame)):
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