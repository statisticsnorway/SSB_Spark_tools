def traverse_hieark(keylist, travdf, parqdf, idstreng):
    global ds_dict
    id = travdf + "_id"
    if (len(keylist)>0):
        varList = list(keylist)
        varList.append(travdf)
        df = parqdf.select(varList)
        if str(df.schema[travdf].dataType)[:20] == "ArrayType(StructType":
            df = df.withColumn('sonavn', F.explode(travdf))
        elif str(df.schema[travdf].dataType)[:15] == "StructType(List":
            df = df.withColumnRenamed(travdf, 'sonavn')
    
        df = df.withColumn(id, (F.monotonically_increasing_id()+ 1000000).cast(StringType()))
        sosublist = df.select('sonavn.*').columns
    
        newlist= []
        renvar= []
    
        for a in sosublist:
            if a in keylist:
                b = '{}_tmpid'.format(a)
                df = df.withColumnRenamed(a, b)
                renvar.append(b)
                newlist.append(b)
    
        for k in keylist:
            if "{}_tmpid".format(k) not in renvar:
                newlist.append(k)
        
        nyvarList = list(newlist)
        nyvarList.append(id)
        nyvarList.append('sonavn.*')
        df= df.select(nyvarList)
        
        for b in renvar:
                a = b[:-6]
                df = df\
                    .withColumnRenamed(a,"{}_{}".format(a,travdf))\
                    .withColumnRenamed(b,a)
        
    else:
        df = parqdf.select(travdf)
        if str(df.schema[travdf].dataType)[:20] == "ArrayType(StructType":
            df = df.withColumn('sonavn', F.explode(travdf))
        elif str(df.schema[travdf].dataType)[:15] == "StructType(List":
            df = df.withColumnRenamed(travdf, 'sonavn')
        df = df.withColumn(id, (F.monotonically_increasing_id()+ 1000000).cast(StringType()))\
                .select(id, 'sonavn.*')
    
   
    cols = [i.name for i in df.schema.fields if ("ArrayType(StructType"==str(i.dataType)[:20]) | ("StructType(List"==str(i.dataType)[:15])]
    
    for socol in cols:
        idstreng.append(socol)
        keylist.append(id)
        traverse_hieark(keylist, socol, df, idstreng)
        keylist.remove(id)
        df = df.drop(socol)
        idstreng.remove(socol)
        
    dictName = '_'.join(map(str, idstreng)) 
    ds_dict[dictName]= df.cache()
        
def pakkut_parq(parqdf, rootdf=False, rootvar=True):
    ##parqdf -- Parquetfilen som skal pakkes ut
    
    ##rootdf: True/False: Avgjør om det skal lages et eget datasett for rotnivå uten variablene som skal pakkes ut
    ##        Liste: Lager et eget datasett med variabler som ligger rot definert i en liste
    
    ##rootvar avgjør hvordan vi skan håndtere variabler på rotnivå. 
    ## True/False: True -- (default) Her vil alle variabler, som ikke må pakkes ut, på rotnivå også bli med på alle datasett som pakkes ut
    ##             False -- Variabler på rotnivå vil kun være  med i datasett for roten hvis rootdf er satt til True 
    ## List: Liste med variabler som skal være med fra rotnivå til datasettene som pakkes ut
    
    global ds_dict
    ds_dict = {}
    keylist = []
    

    
    if type(rootdf) not in (bool, list):
        print("Error: rootdf i pakkut_parq kan bare inneholde boolsk eller list verdier")
        return None
    if type(rootvar) not in (bool, list):
        print("Error: rootvar i pakkut_parq kan bare inneholde boolsk eller list verdier")
        return None
    
    if (rootdf!=False):
        if rootdf==True:
            for i in parqdf.schema.fields:
                if (str(i.dataType)[:20]!="ArrayType(StructType") & (str(i.dataType)[:15]!="StructType(List"):
                    keylist.append(i.name)    
        else:
            keylist=rootdf
        
        if len(keylist)>0:
            ds_dict['rootdf'] = parqdf.select(keylist)
            
    keylist= []
    if (rootvar!=False):
        if (rootvar==True):
            for i in parqdf.schema.fields:
                if (str(i.dataType)[:20]!="ArrayType(StructType") & (str(i.dataType)[:15]!="StructType(List"):
                    keylist.append(i.name)
        else:
            keylist=rootvar
    else:
        keylist=[]
        
    list_col = [i.name for i in parqdf.schema.fields if ("ArrayType(StructType"==str(i.dataType)[:20]) | ("StructType(List"==str(i.dataType)[:15])]
    
    for socol in list_col:
        idstreng = [socol]
        traverse_hieark(keylist, socol, parqdf, idstreng)
    return ds_dict.copy()
    
def kkMissing(missingdata, exceptionData=[]):

    pdDF = pd.DataFrame(columns=['index', 'False', 'True', 'datasett', 'antKorrigert'])
    
    def dfMissing(datadf, name, numlist, boollist):
        df = datadf.toPandas()
        df = pd.isna(df)
        df = df.apply(pd.value_counts).fillna(0)
        df = df.transpose() 
        if 1 in list(df.columns):
                df.rename(columns={1:"antMissing"}, inplace=True)
        else:
            df['antMissing'] = 0
        df['antMissing'].astype(int)
        df['datasett'] = name
        df['variabel'] = df.index
        df['antKorrigert']= 0
        
        df.loc[df['variabel'].isin(numlist) | df['variabel'].isin(boollist), 'antKorrigert'] = df['antMissing']
        
        datadf = datadf.fillna(0, subset=numlist)
        datadf = datadf.fillna(False, subset=boollist)
        return datadf, df
    
    if (isinstance(missingdata, (DataFrame, type({})))) & (isinstance(exceptionData, (type([]), type({})))):
        if (isinstance(missingdata, DataFrame)) & (isinstance(exceptionData, type([]))) | ((isinstance(missingdata, type({}))) & ((isinstance(exceptionData, type({}))) | (isinstance(exceptionData, type([])) & (len(exceptionData)==0)))):
        
            if isinstance(missingdata, type({})):
                
                missdata = missingdata.copy()
                if (isinstance(exceptionData, type([]))):
                    exceptionData = {}
                
                for so in missdata.keys():
                    numlist = []
                    boollist = []
                    
                    for k in missdata[so].schema.fields:
                        if so in exceptionData.keys():
                            if k.name not in exceptionData[so]:
                                if str(k.dataType) in ['LongType', 'ByteType', 'ShortType', 'IntegerType', 'FloatType', 'DoubleType', 'DecimalType']:
                                    numlist.append(k.name)
                                if (str(k.dataType) == 'BooleanType'):
                                    boollist.append(k.name)
                            
                        else:
                            if str(k.dataType) in ['LongType', 'ByteType', 'ShortType', 'IntegerType', 'FloatType', 'DoubleType', 'DecimalType']:
                                numlist.append(k.name)
                            if (str(k.dataType) == 'BooleanType'):
                                boollist.append(k.name)
                                
                    missdata[so], logDF = dfMissing(missdata[so], so, numlist, boollist)
                    
                    if pdDF.empty:
                        pdDF = logDF
                    else: 
                        pdDF = pdDF.append(logDF, ignore_index=True, sort=False)
                    
                pdDF = pdDF.fillna(0)
                
                
                spark_logdf = spark.createDataFrame(pdDF)    
                spark_logdf = spark_logdf.withColumn('andelMissing', F.col('antMissing')/(F.col('antMissing') + F.col('False')))
                spark_logdf = spark_logdf.withColumn('nyandelMissing', (F.col('antMissing') - F.col('antKorrigert'))/(F.col('antMissing') + F.col('False')))
                spark_logdf = spark_logdf.select('datasett', 'variabel', 'antMissing', 'andelMissing', 'antKorrigert', 'nyandelMissing')
                spark_logdf = spark_logdf.withColumn("antMissing", spark_logdf["antMissing"].cast(IntegerType()))
                spark_logdf = spark_logdf.withColumn("antKorrigert", spark_logdf["antKorrigert"].cast(IntegerType()))
                
                return missdata, spark_logdf
            
            else:
                
                missdata = missingdata
                
                numlist = []
                boollist = []
                
                for variabel in missdata.columns:
                    if (len(exceptionData)==0) | (variabel not in exceptionData):
                            if str(missdata.schema[variabel].dataType) in ['LongType', 'ByteType', 'ShortType', 'IntegerType', 'FloatType', 'DoubleType', 'DecimalType']:
                                numlist.append(variabel)
                            if str(missdata.schema[variabel].dataType) == 'BooleanType':
                                boollist.append(variabel)
                
                missdata, logDF = dfMissing(missdata, 'test', numlist, boollist)
                
                if pdDF.empty:
                        pdDF = logDF
                else: 
                    pdDF = pdDF.append(logDF, ignore_index=True, sort=False)
                        
                pdDF = pdDF.fillna(0)
                
                spark_logdf = spark.createDataFrame(pdDF)    
                spark_logdf = spark_logdf.withColumn('andelMissing', F.col('antMissing')/(F.col('antMissing') + F.col('False')))
                spark_logdf = spark_logdf.withColumn('nyandelMissing', (F.col('antMissing') - F.col('antKorrigert'))/(F.col('antMissing') + F.col('False')))
                spark_logdf = spark_logdf.select('variabel', 'antMissing', 'andelMissing', 'antKorrigert', 'nyandelMissing')
                spark_logdf = spark_logdf.withColumn("antMissing", spark_logdf["antMissing"].cast(IntegerType()))
                spark_logdf = spark_logdf.withColumn("antKorrigert", spark_logdf["antKorrigert"].cast(IntegerType()))
                
                return missdata, spark_logdf
                                
        else:
            if (isinstance(missingdata, DataFrame)) & (isinstance(exceptionData, type({}))):
                raise Exception('Når missingdata er en dataframe må exceptionData også være en liste')
                return
            else:
                raise Exception('Når missingdata er en dictionary må exceptionData også være en dictionary')
                return
    else: 
        if isinstance(missingdata, (DataFrame, type({}))):
            raise Exception('exceptionData må være liste eller dictionary')
            return
        else:
            raise Exception('missingdata må være liste eller dictionary')
            return
        
def tverrsnitt(df, coDate=None):
    if coDate!=None:
        df = df.filter(F.col('hendelsetidspunkt') <= coDate)
    df_dato = df.join(df.groupBy('identifikator', 'gjelderPeriode').agg(F.max('hendelsetidspunkt').alias("hendelsetidspunkt")), ['identifikator', 'gjelderPeriode', 'hendelsetidspunkt'], how='inner')
    return df_dato