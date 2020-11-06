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


def listcode_lookup(df, luvar, kodeliste, nokkelverdi, spark_session=None):
    '''
    
    This function adds a new variable to a given dataframe. The added variable contains values 
    that correspond to the values in a variable in the original dataset. For example; if the
    variable on the original dataset is the municipality code, then the function can add
    the name of the municipality corresponding to the municipality code.
     
    :param df: The dataframe containing the variable of interest
    :param luvar: the name of the variable of interest in the supplied dataframe  
    :param kodeliste: the list of codes to use for lookup 
    :param nokkelverdi: Python list ([key value, lookup value]). Key value is the name of the 
                        variable in the list of codes which corresponds to the variable of 
                        interest in the dataframe. Lookup value is the name of the variable
                        in the list of codes to be returned and added to the dataframe. 
    :param spark_session: defines the Spark session to be used by the function. Default is None, which
                        means the function tries to catch the current session.
    :type df: Spark dataframe
    :type luvar: string  
    :type kodeliste: dataframe
    :type nokkelverdi: list 
    :type spark_session: string
    
    Returns: 
    Dataframe: Spark dataframe consisting of the original dataframe updated with a new variable 
              containing the lookup values corresponding to the values of the variable luvar.
    
    '''  
    
    
    #Sjekker om parametre er av korrekt format
    if (isinstance(df, DataFrame)) & (isinstance(luvar, str)) & (isinstance(kodeliste, DataFrame)) & (isinstance(nokkelverdi, type([]))):
    
        #Inititerer variabler
        if spark_session is None:
            spark = SparkSession.builder.getOrCreate()            
        else:
            spark = spark_session
            
        kodeliste_dict = {}

        #Henter nøkkelvariabel og oppslagsvariabel og lager en dictionary av det 
        for row in kodeliste.rdd.collect():
            kodeliste_dict[row[nokkelverdi[0]]] = row[nokkelverdi[1]]

        #Gjør oppslag mot dictionary på variabel vi ønsker og oppretter en egen variabel for resultatet av oppslaget
        mapping_expr = F.create_map([F.lit(x) for x in chain(*kodeliste_dict.items())])
        df = df.withColumn(nokkelverdi[1].format(luvar), mapping_expr.getItem(F.col(luvar))) 

        #Returnere datasettet med ny variabel som resultat av oppslag
        return df
    else:
            #Hvis ikke parametre sendt med funksjonen er korrekt format skrives det ut en feilmelding
            if not (isinstance(df, DataFrame)):
                raise Exception('Første parameter må være en dataframe som har variabelen som skal brukes til å slå opp i kodeliste')
                return
            if not (isinstance(luvar, str)):
                raise Exception('Andre parameter må være en string med navnet på variabel som skal brukes til å slå opp i kodeliste')
                return

            if not (isinstance(kodeliste, DataFrame)):
                raise Exception('Tredje parameter må være en dataframe som inneholder kodelisten variabel skal slå opp i')
                return
            
            if not (isinstance(kodeliste, type([]))):
                raise Exception('Fjerde parameter må være en python liste der første verdi er nøkkelvariabel i kodeliste som variabelen\
                                sammenlignes med og den andre oppslagsvariabel som inneholder verdiene vi ønsker å få med på egen variabel tilbake')
                return
            
            
def missing_correction_bool(df, correction_value=False, exception_for=[], df_name='', spark_session=None):
    '''
    
    This function checks a dataframe for missing values on Boolean variables and replaces missing values with the 
    correction_value parameter. Correction_value defaults to False if no value is given.
    The function corrects all Boolean variables except those specified in the exception_for parameter. 
    All changes are logged and returned.
   
    :param df: The dataframe to be corrected
    :param correction_value: value to replace missing
    :param exception_for: list of variables not to be corrected 
    :param spark_session: defines the Spark session to be used by the function. Default is None, which
                        means the function tries to catch the current session.
    :type df: Spark dataframe
    :type correction_value: Boolean 
    :type exception_for: list
    :type spark_session: string
    
    Returns: corrected dataframe and log in that order
    Dataframe: corrected dataframe (Spark) 
    Dataframe: dataframe (Pandas) with log of number of corrections per variable
    
    '''  
   
    
    if (isinstance(df, DataFrame)) & (isinstance(correction_value, bool)) & (isinstance(exception_for, type([]))):
        #initialiserer variabler
        if spark_session is None:
            spark = SparkSession.builder.getOrCreate()            
        else:
            spark = spark_session
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
        df_log = []
        if len(df_name)>0:
            corrected_missing_log = pd.DataFrame(columns=['dataframe', 'datatype', 'variabel', 'korrigeringer'])
            for k, v in df_count.items():
                if v != 0:
                    df_dict_count= {}
                    df_dict_count['dataframe'] = df_name
                    df_dict_count['variabel'] = k
                    df_dict_count['datatype'] = 'boolean'
                    df_dict_count['korrigeringer'] = v
                    df_log.append(df_dict_count)
        else:
            corrected_missing_log = pd.DataFrame(columns=['datatype', 'variabel', 'korrigeringer'])
            for k, v in df_count.items():
                if v != 0:
                    df_dict_count= {}
                    df_dict_count['variabel'] = k
                    df_dict_count['datatype'] = 'boolean'
                    df_dict_count['korrigeringer'] = v
                    df_log.append(df_dict_count)
                    
        df_log_df = pd.DataFrame(df_log)            
        
        #Korrigerer verdier som er boolske
        df = df.fillna(correction_value, subset=boollist)

        #Returnerer korrigert dataframe (spark) og dictionary med log over antall korrigeringer per variabel 
        return df, df_log_df
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

                                    
def missing_correction_number(df, correction_value=0, exception_for=[], df_name='', spark_session=None):
    '''
    This function checks a dataframe for missing values on numeric variables and replaces missing values with the 
    correction_value parameter. Correction_value defaults to zero if no value is given.
    The function corrects all numeric variables except those specified in the exception_for parameter. 
    All changes are logged and returned.
   
    :param df: The dataframe to be corrected
    :param correction_value: value to replace missing
    :param exception_for: list of variables not to be corrected 
    :param spark_session: defines the Spark session to be used by the function. Default is None, which
                        means the function tries to catch the current session.               
    :type df: Spark dataframe
    :type correction_value: numeric 
    :type exception_for: list
    :type spark_session: string
    
    Returns: corrected dataframe and log in that order
    Dataframe: corrected dataframe (Spark) 
    Dataframe: dataframe (Pandas) with log of number of corrections per variable
    
    
    '''
    

    if (isinstance(df, DataFrame)) & (isinstance(correction_value, numbers.Number)) & (isinstance(exception_for, type([]))):
        #initialiserer variabler
        if spark_session is None:
            spark = SparkSession.builder.getOrCreate()            
        else:
            spark = spark_session
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
        
        df_log = []
        if len(df_name)>0:
            corrected_missing_log = pd.DataFrame(columns=['dataframe', 'datatype', 'variable', 'corrections'])
            for k, v in df_count.items():
                if v != 0:
                    df_dict_count= {}
                    df_dict_count['dataframe'] = df_name
                    df_dict_count['variable'] = k
                    df_dict_count['datatype'] = 'numeric'
                    df_dict_count['corrections'] = v
                    df_log.append(df_dict_count)
        else:
            corrected_missing_log = pd.DataFrame(columns=['datatype', 'variable', 'corrections'])
            for k, v in df_count.items():
                if v != 0:
                    df_dict_count= {}
                    df_dict_count['variable'] = k
                    df_dict_count['datatype'] = 'numeric'
                    df_dict_count['corrections'] = v
                    df_log.append(df_dict_count)

        df_log_df = pd.DataFrame(df_log)
        
        #Korrigerer verdier som er numeriske
        df = df.fillna(correction_value, subset=numlist)

        #Returnerer korrigert dataframe (spark) og dictionary med log over antall korrigeringer per variabel
        return df, df_log_df
                            
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

def spark_missing_correction_bool(df, correction_value=False, exception_for=[], df_name='', spark_session=None):
    #Parameter df --> datasett som det skal kjøres opptelling av missing for
    #Paramater correction_value --> hvilken verdi som skal settes inn istedenfor missing
    #Parameter exception_for --> liste over variable som ikke skal korrigeres
   
    '''
    This function checks a dataframe for missing values on Boolean variables and replaces missing values with the 
    correction_value parameter. Correction_value defaults to False if no value is given.
    The function corrects all Boolean variables except those specified in the exception_for parameter. 
    All changes are logged and returned.
   
    :param df: The dataframe to be corrected
    :param correction_value: value to replace missing
    :param exception_for: list of variables not to be corrected 
    :param spark_session: defines the Spark session to be used by the function. Default is None, which
                        means the function tries to catch the current session.
    :type df: Spark dataframe
    :type correction_value: Boolean 
    :type exception_for: list
    :type spark_session: string
    
    Returns: corrected dataframe and log in that order
    Dataframe: corrected dataframe (Spark) 
    Dataframe: dataframe (Spark) with log of number of corrections per variable
    
    
    '''  
    if (isinstance(df, DataFrame)) & (isinstance(correction_value, bool)) & (isinstance(exception_for, type([]))):
        #initialiserer variabler
        if spark_session is None:
            spark = SparkSession.builder.getOrCreate()            
        else:
            spark = spark_session
            
        boollist = []

        #Legger alle boolske variabler i en egen liste
        for k in df.schema.fields:
            if k.name not in exception_for:
                if (str(k.dataType) == 'BooleanType'):
                    boollist.append(k.name)
        #Lager en logg 
        df_count = df[boollist].select([F.count(F.when(F.isnull(c), c)).alias(c) for c in df[boollist].columns])
        df_columns = df_count.columns
        df_log = []
        
        if len(df_name)>0:
            missing_variabler = [StructField('dataframe', StringType(), False),\
                   StructField('variable', StringType(), True),\
                   StructField('datatype', StringType(), True),\
                   StructField('corrections', IntegerType(), False)]
            missing_schema = StructType(missing_variabler)
            

            for row in df_count.rdd.collect():
                for k in df_columns:
                    if row[k]!=0:
                        df_dict_count= {}
                        df_dict_count['dataframe'] = df_name
                        df_dict_count['variable'] = k
                        df_dict_count['datatype'] = 'boolean'
                        df_dict_count['corrections'] = row[k]
                        df_log.append(df_dict_count)
        else:
            missing_variabler = [StructField('variable', StringType(), True),\
                   StructField('datatype', StringType(), True),\
                   StructField('corrections', IntegerType(), False)]
            missing_schema = StructType(missing_variabler)
            
            for row in df_count.rdd.collect():
                for k in df_columns:
                    if row[k]!=0:
                        df_dict_count= {}
                        df_dict_count['variable'] = k
                        df_dict_count['datatype'] = 'boolean'
                        df_dict_count['corrections'] = row[k]
                        df_log.append(df_dict_count)
        
        if len(df_log)>0:
            rdd_missing = spark.sparkContext.parallelize(df_log)
            df_corrections = spark.createDataFrame(rdd_missing, missing_schema)
        else:
            df_corrections = spark.createDataFrame(spark.sparkContext.emptyRDD(), missing_schema)
            
        #Korrigerer verdier som er boolske
        df = df.fillna(correction_value, subset=boollist)

            #Returnerer korrigert dataframe (spark) og dictionary med log over antall korrigeringer per variabel 
        return df, df_corrections
        
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
        
def spark_missing_correction_number(df, correction_value=0, exception_for=[], df_name='', spark_session=None):
    '''
    
    This function checks a dataframe for missing values on numeric variables and replaces missing values with the 
    correction_value parameter. Correction_value defaults to zero if no value is given.
    The function corrects all numeric variables except those specified in the exception_for parameter. 
    All changes are logged and returned.
   
    :param df: The dataframe to be corrected
    :param correction_value: value to replace missing
    :param exception_for: list of variables not to be corrected 
    :param spark_session: defines the Spark session to be used by the function. Default is None, which
                means the function tries to catch the current session.
    :type df: Spark dataframe
    :type correction_value: numeric 
    :type exception_for: list
    :type spark_session: string
    
    Returns: corrected dataframe and log in that order
    Dataframe: corrected dataframe (Spark) 
    Dataframe: dataframe (Spark) with log of number of corrections per variable
    
    '''
    
    if (isinstance(df, DataFrame)) & (isinstance(correction_value, numbers.Number)) & (isinstance(exception_for, type([]))):
        #initialiserer variabler
        if spark_session is None:
            spark = SparkSession.builder.getOrCreate()            
        else:
            spark = spark_session
        numlist = []

        #Legger alle numeriske variabler i en egen liste
        for k in df.schema.fields:
            if k.name not in exception_for:
                if str(k.dataType) in ['LongType', 'ByteType', 'ShortType', 'IntegerType', 'FloatType', 'DoubleType', 'DecimalType']:    
                    numlist.append(k.name)
        
                
        
        df_count = df[numlist].select([F.count(F.when(F.isnull(c), c)).alias(c) for c in df[numlist].columns])
        df_columns = df_count.columns
        df_log = []
        
        if len(df_name)>0:
            missing_variabler = [StructField('dataframe', StringType(), False),\
                   StructField('variable', StringType(), True),\
                   StructField('datatype', StringType(), True),\
                   StructField('corrections', IntegerType(), False)]
            missing_schema = StructType(missing_variabler)
            

            for row in df_count.rdd.collect():
                for k in df_columns:
                    if row[k]!=0:
                        df_dict_count= {}
                        df_dict_count['dataframe'] = df_name
                        df_dict_count['variable'] = k
                        df_dict_count['datatype'] = 'numeric'
                        df_dict_count['corrections'] = row[k]
                        df_log.append(df_dict_count)
        else:
            missing_variabler = [StructField('variabel', StringType(), True),\
                   StructField('datatype', StringType(), True),\
                   StructField('corrections', IntegerType(), False)]
            missing_schema = StructType(missing_variabler)
            
            for row in df_count.rdd.collect():
                for k in df_columns:
                    if row[k]!=0:
                        df_dict_count= {}
                        df_dict_count['variable'] = k
                        df_dict_count['datatype'] = 'numeric'
                        df_dict_count['corrections'] = row[k]
                        df_log.append(df_dict_count)
        
        if len(df_log)>0:
            rdd_missing = spark.sparkContext.parallelize(df_log)
            df_corrections = spark.createDataFrame(rdd_missing, missing_schema)
        else:
            df_corrections = spark.createDataFrame(spark.sparkContext.emptyRDD(), missing_schema)
        
             
        #Korrigerer verdier som er numeriske
        df = df.fillna(correction_value, subset=numlist)

        #Returnerer korrigert dataframe (spark) og dictionary med log over antall korrigeringer per variabel
        return df, df_corrections
        
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

