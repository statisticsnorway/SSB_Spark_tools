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