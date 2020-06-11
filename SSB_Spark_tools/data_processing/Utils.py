import pyspark.sql.functions as F
from pyspark.sql.types import *
    
def traverse_hierarchy(keylist, travdf, parqdf, idstreng, hierarchylevels):
    '''
    This function walks a hierarcical datasett stored in memmory,
    and returns all packed as unpackked data objects.
    
    The function takes a hirarchical datasett, investigates the schema structre 
    to find any othe hirarchy ellements of type struct, list,and arrays. 
    All object found in thhe data obdject are subsequently unnpacked, 
    and returend as an data object. 
    
    This is a recursive  function, which ends, when there are no list or array type 
    columns left in the original data object. 
    
    The function also allows for level control limiting the depth at which the 
    function will look for arrays.
    
    :param keylist: List containing the variables you want to carry forward from the level above, to the level bellow
    :type keylist: List
    :param 
    '''
    
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
    
    hierarchylevels = hierarchylevels - 1
    if (hierarchylevels == -1) | (hierarchylevels != 0):
        cols = [i.name for i in df.schema.fields if ("ArrayType(StructType"==str(i.dataType)[:20]) | ("StructType(List"==str(i.dataType)[:15])]

        for socol in cols:
            idstreng.append(socol)
            keylist.append(id)
            traverse_hierarchy(keylist, socol, df, idstreng, hierarchylevels)
            keylist.remove(id)
            df = df.drop(socol)
            idstreng.remove(socol)
        
    dictName = '_'.join(map(str, idstreng)) 
    ds_dict[dictName]= df.cache()
        
def unpack_parquet(parqdf, rootdf=False, rootvar=True, levels=-1):
    '''
    
    :param parqdf: -- Parquetfilen som skal pakkes ut
    :type parqdf:
    :param rootdf: True/False: Avgjør om det skal lages et eget datasett for rotnivå uten variablene som skal pakkes ut
    :type rootdf: Boolean/list Liste: Lager et eget datasett med variabler som ligger rot definert i en liste
    
    rootvar avgjør hvordan vi skan håndtere variabler på rotnivå. 
     True/False: True -- (default) Her vil alle variabler, som ikke må pakkes ut, på rotnivå også bli med på alle datasett som pakkes ut
                 False -- Variabler på rotnivå vil kun være  med i datasett for roten hvis rootdf er satt til True 
     List: Liste med variabler som skal være med fra rotnivå til datasettene som pakkes ut
    '''
    global ds_dict
    ds_dict = {}
    keylist = []
    
    hierarchylevels = levels
    
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
    
    if hierarchylevels != 0:
        list_col = [i.name for i in parqdf.schema.fields if ("ArrayType(StructType"==str(i.dataType)[:20]) | ("StructType(List"==str(i.dataType)[:15])]

        for socol in list_col:
            idstreng = [socol]
            traverse_hierarchy(keylist, socol, parqdf, idstreng, hierarchylevels)
            
    return ds_dict.copy()
        
def cross_sectional(df, event_var, event_id, coDate=None):
    if coDate!=None:
        df = df.filter(F.col(event_var) <= coDate)
    
    eventlist_ids = event_id.copy()
    eventlist_ids.append(event_var)
    
    df_cross = df.join(df.groupBy(event_id).agg(F.max(event_var).alias(event_var)),\
                          eventlist_ids, how='inner')
    return df_cross