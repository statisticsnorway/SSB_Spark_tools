from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from itertools import chain
import pyspark.sql.functions as F
from pyspark.sql.column import Column

def listcode_check(df, variabel, kodeliste, spark_session=None):
    '''
    
    This function checks the values of a variable in a dataframe against a given list of codes
    and makes a new dataframe containing these values, a count of how many times the value occurs
    and a variable indicating whether or not this value existed in the code list or not.
    In addition, the function returns a Boolean variable indicating whether the variable included has
    one or more values outside the code list or not.
     
    :param df: Spark dataframe containing variable to be controlled
    :param variabel: variable to be checked against the code list 
    :param kodeliste: list of codes that the variable should be checked against, sent as a Python list or 
                        as a dataframe with listcode values in first column of the dataframe
    :param spark_session: defines the Spark session to be used by the function. Default is None, which
                        means the function tries to catch the current session.
    
    :type df: Spark dataframe
    :type variabel: string 
    :type kodeliste: list or dataframe
    :type spark_session: string
    
    Returns: Boolean variable and a dataframe in that order.
    Boolean variable: A Boolean variable indicating wheteher or not there exists one or more values in
               the variable that did not exist in the codelist.
    Dataframe: A data frame (Spark) consisting of the values found in the variabel,
               the number of times the value occurs and a Boolean value telling wheter or not
               the value exists in the relevant code list or not.
    
    '''  
 
    #Sjekker om parametre er av korrekt format       
    if (isinstance(df, DataFrame)) & (isinstance(variabel, str)) & ((isinstance(kodeliste, type([]))) | (isinstance(kodeliste, DataFrame))):
            
        #Setter opp peker til context som brukes i forbindelse med oppretting av nytt datasett
        if spark_session is None:
            spark = SparkSession.builder.getOrCreate()            
        else:
            spark = spark_session
        
        #initialiserer variabler
        sjekk_listedf = []
        sjekk_bol = True
        
        #Hvis kodeliste er dataframe gjøres første kolonne (bør også være eneste kolonne) om til liste
        if isinstance(kodeliste, DataFrame):
            kodeliste = [row[0] for row in kodeliste.collect()]
        
        #Grupperer variabelene i datasettet og teller opp instanser av ulike verdier
        koder_df = df.groupby(variabel).count().withColumnRenamed('count', 'antall')
        
        #Går gjennom verdier på variabel og sjekker om de er med i kodeliste, for hver verdi blir det laget en record  
        #Hvis det finnes 1 eller flere verdier som ikke er i kodelisten blir en boolsk verdi (sjekk_bol) satt til False
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
        
        #Oppretter dataframe for resultatet av gjennomgangen ovenfor
        field_kl = [StructField('kode', StringType(), False),\
                        StructField('antall', IntegerType(), True),\
                        StructField('i_kodeliste', BooleanType(), False)]
        schema_kl = StructType(field_kl)
        rdd_sl = spark.sparkContext.parallelize(sjekk_listedf)
        sjekk_df = spark.createDataFrame(rdd_sl, schema_kl)
        
        #Returner opprettet dataframe og boolsk verdi
        return sjekk_bol, sjekk_df
    else:
        #Hvis ikke parametre sendt med funksjonen er korrekt format skrives det ut en feilmelding
        if not (isinstance(df, DataFrame)):
            raise Exception('Første parameter må være en dataframe som har variabelen som skal sjekkes')
            return
        if not (isinstance(variabel, str)):
            raise Exception('Andre parameter må være en string med navnet på variabel som skal sjekkes')
            return
        
        if not (isinstance(kodeliste, type([]))):
            raise Exception('Tredje parameter må være en python liste som inneholder kodelisten \
                            variabel skal sjekkes mot eller en dataframe der første kolonne er kodelisten\
                            som det skal sjekkes mot')
            return

def compare_dimdf(df1, df2):
    '''
    This function checks if two dataframes have indentical (number of columns and column names) columns, and the same amount of rows.
    :param df1: First dataframe for comparison
    :param df2: Second dataframe for comparison
    
    Returns: Boolean variable indicating if number of variables, name of variables and the amount of rows between
    the two dataframes are identical
    '''
    test = True
    if (sorted(list(df1.columns)) != sorted(list(df2.columns))):
        test = False
    if (df1.count() != df2.count()):
        test = False
    return test

def compare_columns(df1, df2):
    '''
    This function checks if two dataframes have indentical (number of columns and column names) columns
    :param df1: First dataframe for comparison
    :param df2: Second dataframe for comparison
    
    Returns: Boolean variable indicates if columns of two dataframes are identical
    '''
    test = False
    if (sorted(list(df1.columns)) == sorted(list(df2.columns))):
        test = True
    return test

def compare_df(df1, df2):
    '''
    This functions compares two dataframes to check if they are identical
    :param df1: First dataframe for comparison
    :param df2: Second dataframe for comparison
    
    Returns: Boolean variable indicates if two dataframes are identical
    '''
    return (len(df1.subtract(df2).head(1)) == 0)
