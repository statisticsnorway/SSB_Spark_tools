from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from itertools import chain
import pyspark.sql.functions as F
from pyspark import SparkContext
import pandas as pd
from collections import OrderedDict
import numbers
from pyspark.sql import DataFrame
from functools import reduce

def listcode_lookup(df, variabel, kodeliste, nokkelverdi):
    #Parameter input er:
    #                    df --> datasett som inneholder variabel med verdier som skal slås opp i kodeliste
    #                    variabel --> Variabel med verdier som skal slås opp i kodeliste
    #                    kodeliste --> kodeliste som det skal slås opp i, sendt som spark dataframe
    #                    nokkelverdi --> python liste ([nøkkelverdi, oppslagsverdi]) som har variabel som inneholder nøkkelverdier som variabel       
    #                                    (angitt ovenfor) sammenlignes mot og variabel som inneholder oppslagsverdi som vi ønsker tilbake som egen 
    #                                    variabel på vår datasett
    
    #Sjekker om parametre er av korrekt format
    if (isinstance(df, DataFrame)) & (isinstance(variabel, str)) & (isinstance(kodeliste, DataFrame)) & (isinstance(nokkelverdi, type([]))):
    
        #Inititerer variabler
        kodeliste_dict = {}

        #Henter nøkkelvariabel og oppslagsvariabel og lager en dictionary av det 
        for row in kodeliste.rdd.collect():
            kodeliste_dict[row[nokkelverdi[0]]] = row[nokkelverdi[1]]

        #Gjør oppslag mot dictionary på variabel vi ønsker og oppretter en egen variabel for resultatet av oppslaget
        mapping_expr = F.create_map([F.lit(x) for x in chain(*kodeliste_dict.items())])
        df = df.withColumn("{}_kodelisteverdi".format(variabel), mapping_expr.getItem(F.col(variabel))) 

        #Returnere datasettet med ny variabel som resultat av oppslag
        return df
    else:
            #Hvis ikke parametre sendt med funksjonen er korrekt format skrives det ut en feilmelding
            if not (isinstance(df, DataFrame)):
                raise Exception('Første parameter må være en dataframe som har variabelen som skal brukes til å slå opp i kodeliste')
                return
            if not (isinstance(variabel, str)):
                raise Exception('Andre parameter må være en string med navnet på variabel som skal brukes til å slå opp i kodeliste')
                return

            if not (isinstance(kodeliste, DataFrame)):
                raise Exception('Tredje parameter må være en dataframe som inneholder kodelisten variabel skal slå opp i')
                return
            
            if not (isinstance(kodeliste, type([]))):
                raise Exception('Fjerde parameter må være en python liste der første verdi er nøkkelvariabel i kodeliste som variabelen\
                                sammenlignes med og den andre oppslagsvariabel som inneholder verdiene vi ønsker å få med på egen variabel tilbake')
                return
            
            
def missing_correction_bool(df, correction_value=False, exception_for=[]):
    #Parameter df --> datasett som det skal kjøres opptelling av missing for
    #Paramater correction_value --> hvilken verdi som skal settes inn istedenfor missing
    #Parameter exception_for --> liste over variable som ikke skal korrigeres
    
    if (isinstance(df, DataFrame)) & (isinstance(correction_value, bool)) & (isinstance(exception_for, type([]))):
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
    else:
        if not (isinstance(df, DataFrame)):
            raise Exception('Parameter df må være en dataframe')
            return
        if not (isinstance(correction_value, numbers.Number)):
            raise Exception('Parameter correction_value må være boolsk format')
            return
        if not (isinstance(exception_for, type([]))):
            raise Exception('Parameter exception_for må være liste format')
            return

                                    
def missing_correction_number(df, correction_value=0, exception_for=[]):
    #Parameter df --> datasett som det skal kjøres opptelling av missing for
    #Paramater correction_value --> hvilken verdi som skal settes inn istedenfor missing
    #Parameter exception_for --> liste over variable som ikke skal korrigeres

    if (isinstance(df, DataFrame)) & (isinstance(correction_value, numbers.Number)) & (isinstance(exception_for, type([]))):
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
                            
    else:
        if not (isinstance(df, DataFrame)):
            raise Exception('Parameter df må være en dataframe')
            return
        if not (isinstance(correction_value, numbers.Number)):
            raise Exception('Parameter correction_value må være numerisk format')
            return
        if not (isinstance(exception_for, type([]))):
            raise Exception('Parameter exception_for må være liste format')
            return

def spark_missing_correction_bool(df, correction_value=False, exception_for=[]):
    #Parameter df --> datasett som det skal kjøres opptelling av missing for
    #Paramater correction_value --> hvilken verdi som skal settes inn istedenfor missing
    #Parameter exception_for --> liste over variable som ikke skal korrigeres
    
    if (isinstance(df, DataFrame)) & (isinstance(correction_value, bool)) & (isinstance(exception_for, type([]))):
        #initialiserer variabler
        boollist = []

        #Legger alle boolske variabler i en egen liste
        for k in df.schema.fields:
            if k.name not in exception_for:
                if (str(k.dataType) == 'BooleanType'):
                    boollist.append(k.name)
        #Lager en logg 
        df_count = df[boollist].select([F.count(F.when(F.isnull(c), c)).alias(c) for c in df[boollist].columns])
        df_columns = df_count.columns
        df_dict_count= {}
        for row in df_count.rdd.collect():
            for k in df_columns:
                if row[k]!=0:
                    df_dict_count[k] = row[k]
                
        #Korrigerer verdier som er boolske
        df = df.fillna(correction_value, subset=boollist)

        #Returnerer korrigert dataframe (spark) og dictionary med log over antall korrigeringer per variabel 
        return df, df_dict_count
    else:
        if not (isinstance(df, DataFrame)):
            raise Exception('Parameter df må være en dataframe')
            return
        if not (isinstance(correction_value, numbers.Number)):
            raise Exception('Parameter correction_value må være boolsk format')
            return
        if not (isinstance(exception_for, type([]))):
            raise Exception('Parameter exception_for må være liste format')
            return

                                    
def spark_missing_correction_number(df, correction_value=0, exception_for=[]):
    #Parameter df --> datasett som det skal kjøres opptelling av missing for
    #Paramater correction_value --> hvilken verdi som skal settes inn istedenfor missing
    #Parameter exception_for --> liste over variable som ikke skal korrigeres

    if (isinstance(df, DataFrame)) & (isinstance(correction_value, numbers.Number)) & (isinstance(exception_for, type([]))):
        #initialiserer variabler
        numlist = []

        #Legger alle numeriske variabler i en egen liste
        for k in df.schema.fields:
            if k.name not in exception_for:
                if str(k.dataType) in ['LongType', 'ByteType', 'ShortType', 'IntegerType', 'FloatType', 'DoubleType', 'DecimalType']:    
                    numlist.append(k.name)
        
        df_count = df[numlist].select([F.count(F.when(F.isnull(c), c)).alias(c) for c in df[numlist].columns])
        df_columns = df_count.columns
        df_dict_count= {}
        for row in df_count.rdd.collect():
            for k in df_columns:
                if row[k]!=0:
                    df_dict_count[k] = row[k]
        
        #Korrigerer verdier som er numeriske
        df = df.fillna(correction_value, subset=numlist)

        #Returnerer korrigert dataframe (spark) og dictionary med log over antall korrigeringer per variabel
        return df, df_dict_count
                            
    else:
        if not (isinstance(df, DataFrame)):
            raise Exception('Parameter df må være en dataframe')
            return
        if not (isinstance(correction_value, numbers.Number)):
            raise Exception('Parameter correction_value må være numerisk format')
            return
        if not (isinstance(exception_for, type([]))):
            raise Exception('Parameter exception_for må være liste format')
            return

def spark_missing_correction_number2(df, correction_value=0, exception_for=[]):
    #Parameter df --> datasett som det skal kjøres opptelling av missing for
    #Paramater correction_value --> hvilken verdi som skal settes inn istedenfor missing
    #Parameter exception_for --> liste over variable som ikke skal korrigeres
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)


    if (isinstance(df, DataFrame)) & (isinstance(correction_value, numbers.Number)) & (isinstance(exception_for, type([]))):
        #initialiserer variabler
        numlist = []

        #Legger alle numeriske variabler i en egen liste
        for k in df.schema.fields:
            if k.name not in exception_for:
                if str(k.dataType) in ['LongType', 'ByteType', 'ShortType', 'IntegerType', 'FloatType', 'DoubleType', 'DecimalType']:    
                    numlist.append(k.name)
        
        df_var_korr = df.select(numlist)
        
        if len(numlist)>0:
            df_dict_count = reduce(
                lambda a, b: a.union(b),
                (
                    df_var_korr.agg(
                        F.count(F.when(F.isnull(c), c)).alias('ant_korrigeringer')
                    ).select(F.lit(c).alias("variabel"), "ant_korrigeringer")
                    for c in df_var_korr.columns
                )
            )
             
            #Korrigerer verdier som er numeriske
            df = df.fillna(correction_value, subset=numlist)

            #Returnerer korrigert dataframe (spark) og dictionary med log over antall korrigeringer per variabel
            return df, df_dict_count
        else:
            missing_variabler = [StructField('variabel', StringType(), True),\
                   StructField('ant_korrigeringer', IntegerType(), False)]
            missing_schema = StructType(missing_variabler)
            empty = sqlContext.createDataFrame(sc.emptyRDD(), missing_schema)
            return df, empty
                            
    else:
        if not (isinstance(df, DataFrame)):
            raise Exception('Parameter df må være en dataframe')
            return
        if not (isinstance(correction_value, numbers.Number)):
            raise Exception('Parameter correction_value må være numerisk format')
            return
        if not (isinstance(exception_for, type([]))):
            raise Exception('Parameter exception_for må være liste format')
            return

def spark_missing_correction_bool2(df, correction_value=False, exception_for=[]):
    #Parameter df --> datasett som det skal kjøres opptelling av missing for
    #Paramater correction_value --> hvilken verdi som skal settes inn istedenfor missing
    #Parameter exception_for --> liste over variable som ikke skal korrigeres
    
    if (isinstance(df, DataFrame)) & (isinstance(correction_value, bool)) & (isinstance(exception_for, type([]))):
        #initialiserer variabler
        boollist = []

        #Legger alle boolske variabler i en egen liste
        for k in df.schema.fields:
            if k.name not in exception_for:
                if (str(k.dataType) == 'BooleanType'):
                    boollist.append(k.name)
        #Lager en logg 
        df_var_korr = df.select(boollist)
        if len(boollist)>0:
            df_dict_count = reduce(
                lambda a, b: a.union(b),
                (
                    df_var_korr.agg(
                        F.count(F.when(F.isnull(c), c)).alias('ant_korrigeringer')
                    ).select(F.lit(c).alias("variabel"), "ant_korrigeringer")
                    for c in df_var_korr.columns
                )
            )

            #Korrigerer verdier som er boolske
            df = df.fillna(correction_value, subset=boollist)

            #Returnerer korrigert dataframe (spark) og dictionary med log over antall korrigeringer per variabel 
            return df, df_dict_count
        else:
            missing_variabler = [StructField('variabel', StringType(), True),\
                   StructField('ant_korrigeringer', IntegerType(), False)]
            missing_schema = StructType(missing_variabler)
            empty = sqlContext.createDataFrame(sc.emptyRDD(), missing_schema)
            return df, empty
    else:
        if not (isinstance(df, DataFrame)):
            raise Exception('Parameter df må være en dataframe')
            return
        if not (isinstance(correction_value, numbers.Number)):
            raise Exception('Parameter correction_value må være boolsk format')
            return
        if not (isinstance(exception_for, type([]))):
            raise Exception('Parameter exception_for må være liste format')
            return
