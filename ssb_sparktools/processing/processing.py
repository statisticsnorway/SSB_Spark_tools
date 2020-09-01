import pyspark.sql.functions as F
from pyspark.sql.types import *

def cross_sectional(df, event_var, event_id, coDate=None):
    '''
    This function makes a cross sectional dataset of the last record before a defined date
    for all uniquely identified units in a dataframe (df).
    The date for which the cross sectional dataset is made is defined in the parameter coDate; 
    if noe date is given, the default date is today.
    The records unique id is defined in the parameter event_id. 
    The time at which the event happens is given in the variable event_var.
    
    :param df: The dataframe of observations from which to make a cross sectional dataset 
    :type df: dataframe 
    :param event_var: a variable containing the date at which the event happened
    :type event_var:  time/date 
    :param event_id: the variables that uniquely define a record
    :type event_id: list
    :param coDate: the date at which the cross sectional dataset is taken.
    :type coDate: time/date 
    
    Returns:a dataframe
    Dataframe: A cross sectional dataframe.
    '''
    if coDate!=None:
        df = df.filter(F.col(event_var) <= coDate)
    
    eventlist_ids = event_id.copy()
    eventlist_ids.append(event_var)
    
    df_cross = df.join(df.groupBy(event_id).agg(F.max(event_var).alias(event_var)),\
                          eventlist_ids, how='inner')
    return df_cross

def traverse_hierarchy(keylist, travdf, parqdf, idstreng, hierarchylevels):
    '''
    This function walks through a hierarcical dataset stored in memory,
    and unpacks all packed data objects.
    
    The function takes a hirarchical dataset, investigates the schema structure 
    to find any other hirarchy elements of type struct, list, and arrays. 
    All objects found in these data objects are subsequently unpacked, 
    and replaces the packed data objects. 
    
    This is a recursive function, which ends when there are no list or array type 
    columns left in the original data object. 
    
    The function also allows for level control limiting the depth at which the 
    function will look for arrays.
    
    :param keylist: List containing the variables you want to carry forward from the level above, to the level below
    :type keylist: list
    :param travdf: data object to unpack
    :type travdf: string
    :param parqdf: The hierarchical dataset that you are investigating 
    :type parqdf: dataframe
    :param idstreng: identifying record in parent dataframe
    :type idstreng: string
    :param hierarchylevels: controls the depth at which the function will look for arrays 
    :type hierarchylevels: numeric value
    
     Returns: 
     None: Returns nothing, but updates dictionary created earlier
    
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
    This function unpacks a hierarchical spark dataframe and relies on function traverse_hierarchy to traverse the hierarchy and unpack.
    Each unpacked object is a dataframe that gets stored in a dictionary which is then returned to the user.
    
    The parameter rootdf is used to decide whether to create a separate root level dataset without the variables to 
    be extracted. 
        Usage: 
        True: a root level dataset is made. 
        False: a root level dataset is not made. 
        List: a dataset with root level variables defined in a list. 
        The default value is False.

    The parameter rootvar is used to tell how the variables at the root level are to be treated.
        Usage: 
        True: all variables, which do not need to be unpacked, at the root level, will also be included in all datasets that are unpacked.
        False: root level variables will not be included in the datasets that are unpacked.
        List: list of variables from root level to be added to the datasets that are unpacked. 
    
    The levels parameter tells the function how many hierarchy levels it will traverse and unpack. 
        Usage:
        Default value is -1, which means that the function traverses the whole hierarchy and unpacks all objects. 
        0: means that it doesn't unpack anything, only the root variables.
        1: the function unpacks one level  
        2: the function unpacks two levels etc.
    
    :param parqdf: The dataframe with hierarchical structure to be unpacked.
    :type parqdf: dataframe 
    :param rootdf: Decides whether to create a separate root level dataset.
    :type rootdf: boolean/list 
    :rootvar: Tells if the root level variables are to be transfered to the unpacked objects or not.
    :type rootvar: boolean/list
    :param levels: Tells the function how many hierarchy levels to be traversed
    :type levels: numerical value
    
     Returns: a dictionary
     Dictionary: A dictionary of unpacked dataframes.
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
        
