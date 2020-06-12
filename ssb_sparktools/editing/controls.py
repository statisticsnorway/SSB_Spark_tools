from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from itertools import chain
import pyspark.sql.functions as F
from pyspark import SparkContext

def listcode_check(df, variabel, kodeliste):
    #Parameter input er:
    #                    df --> datasett som inneholder variabel som skal kontrolleres
    #                    variabel --> Variabel som skal kontrolleres
    #                    kodeliste --> kodeliste som variabel skal sjekkes mot, sendt som python liste
    
    #Sjekker om parametre er korrekt format    
    if (isinstance(df, DataFrame)) & (isinstance(variabel, str)) & (isinstance(kodeliste, type([]))):
        
        #Kopierer over spark context og setter opp peker til context som brukes i forbindelse med oppretting av nytt datasett
        sc = SparkContext.getOrCreate()
        sqlContext = SQLContext(sc)
        
        #initialiserer variabler
        sjekk_listedf = []
        sjekk_bol = True
        
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
        rdd_sl = sc.parallelize(sjekk_listedf)
        sjekk_df = sqlContext.createDataFrame(rdd_sl, schema_kl)
        
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
            raise Exception('Tredje parameter må være en python liste som inneholder kodelisten variabel skal sjekkes mot')
            return
            
