import pandas as pd
from collections import OrderedDict

def missing_df(df, subsetvar=None):
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
    #df['variable'] = df.index
    
    #Endrer rekkefølgen på variablene til ønsket rekkefølge
    #df = df[['variable', 'missing', 'shareoftotal']]
    
    return df

def missing_correction_bool(df, correction_value=False, exception_for=[]):
    #initialiserer variabler
    boollist = []
    
    #Legger alle boolske variabler i en egen liste
    for k in df.schema.fields:
        if k.name not in exception_for:
            if (str(k.dataType) == 'BooleanType'):
                boollist.append(k.name)
    
    #Transformerer spark dataframe til pandas dataframe for å lage logg med opptellinger av korrigerte verdier
    df_count = df[boollist].toPandas()
    df_count = df_count.isnull().sum()
    df_count = df_count.to_dict(OrderedDict)
    df_dict_count= {}
    for k, v in df_count.items():
        if v != 0:
            df_dict_count[k] = v
            
    #Korrigerer verdier som er boolske
    df = df.fillna(correction_value, subset=boollist)
    
    #Returnerer korrigert dataframe (spark) og dictionary med log over antall korrigeringer per variabel 
    return df, df_dict_count
                                    
def missing_correction_number(df, correction_value=0, exception_for=[]):
    #initialiserer variabler
    numlist = []
    
    #Legger alle numeriske variabler i en egen liste
    for k in df.schema.fields:
        if k.name not in exception_for:
            if str(k.dataType) in ['LongType', 'ByteType', 'ShortType', 'IntegerType', 'FloatType', 'DoubleType', 'DecimalType']:    
                numlist.append(k.name)
                
    #Transformerer spark dataframe til pandas dataframe for å lage logg med opptellinger av korrigerte verdier        
    df_count = df[numlist].toPandas()
    df_count = df_count.isnull().sum()
    df_count = df_count.to_dict(OrderedDict)
    df_dict_count= {}
    for k, v in df_count.items():
        if v != 0:
            df_dict_count[k] = v

    #Korrigerer verdier som er numeriske
    df = df.fillna(correction_value, subset=numlist)
    
    #Returnerer korrigert dataframe (spark) og dictionary med log over antall korrigeringer per variabel
    return df, df_dict_count
        