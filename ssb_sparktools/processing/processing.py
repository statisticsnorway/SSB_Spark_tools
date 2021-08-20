from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql import Window
            
def cross_sectional(df, event_var, event_id, coDate=None, spark_session=None):
    '''
    This function makes a cross sectional dataset of the last record before a defined date
    for records defined in event_id.
    The date for which the cross sectional dataset is made is defined in the parameter coDate; 
    if no date is given, the default date is today. 
    The time at which the event happens is given in the variable event_var.
    
    :param df: The dataframe of observations from which to make a cross sectional dataset 
    :type df: dataframe 
    :param event_var: Name of the column in the dataframe containing the date at which the event happened
    :type event_var: string 
    :param event_id: The combination of variables from which the last occurrence on event_var will be returned 
    :type event_id: list
    :param coDate: The max date for events on event_var
    :type coDate: datetime/timestamp
    :param spark_session: defines the Spark session to be used by the function. Default is None, which
                        means the function tries to catch the current session.
    :type spark_session: string
                        
    Returns: a dataframe
    Dataframe: A cross sectional dataframe.
    '''
    if (isinstance(df, DataFrame)) & (isinstance(event_var, str)) & (isinstance(event_id, list)) & \
        ((coDate==None) | (isinstance(coDate, datetime))):
        
        if spark_session is None:
            spark = SparkSession.builder.getOrCreate()            
        else:
            spark = spark_session
        
        if (([dtype for name, dtype in df.dtypes if name == event_var][0])=='timestamp'):
        
            if coDate!=None:
                df = df.filter(F.col(event_var) <= coDate)

            w = Window.partitionBy(event_id).orderBy(F.col(event_var).desc())

            df_cross = (df.withColumn('rn', F.row_number().over(w))
                            .filter(F.col('rn')==1)
                            .drop('rn'))
            return df_cross
        else:
            if not (([dtype for name, dtype in df.dtypes if name == event_var][0])=='timestamp'):
                raise Exception('Variabel event_var i dataframe må ha datatype timestamp')
                return
            
    else:
        if not (isinstance(df, DataFrame)):
                raise Exception('Første parameter må være en dataframe.')
                return
            
        if not (isinstance(event_var, str)):
            raise Exception('Andre parameter må være en string.')
            return

        if not (isinstance(event_id, list)):
            raise Exception('Tredje parameter må være en liste med variable.')
            return

        if not ((coDate==None) | (isinstance(coDate, datetime.datetime))):
            raise Exception('Fjerde parameter må være en dato.')
            return
        

def cross_sectional_old(df, event_var, event_id, coDate=None, spark_session=None):
    '''
    This function makes a cross sectional dataset of the last record before a defined date
    for records defined in event_id.
    The date for which the cross sectional dataset is made is defined in the parameter coDate; 
    if no date is given, the default date is today. 
    The time at which the event happens is given in the variable event_var.
    
    :param df: The dataframe of observations from which to make a cross sectional dataset 
    :type df: dataframe 
    :param event_var: Name of the column in the dataframe containing the date at which the event happened
    :type event_var: string 
    :param event_id: The combination of variables from which the last occurrence on event_var will be returned 
    :type event_id: list
    :param coDate: The max date for events on event_var
    :type coDate: datetime/timestamp
    :param spark_session: defines the Spark session to be used by the function. Default is None, which
                        means the function tries to catch the current session.
    :type spark_session: string
                        
    Returns: a dataframe
    Dataframe: A cross sectional dataframe.
    '''
    if (isinstance(df, DataFrame)) & (isinstance(event_var, str)) & (isinstance(event_id, list)) & \
        ((coDate==None) | (isinstance(coDate, datetime))):
        
        if spark_session is None:
            spark = SparkSession.builder.getOrCreate()            
        else:
            spark = spark_session
        
        if (([dtype for name, dtype in df.dtypes if name == event_var][0])=='timestamp'):
        
            if coDate!=None:
                df = df.filter(F.col(event_var) <= coDate)

            eventlist_ids = event_id.copy()
            eventlist_ids.append(event_var)

            df_cross = df.join(df.groupBy(event_id).agg(F.max(event_var).alias(event_var)),\
                                  eventlist_ids, how='inner')
            return df_cross
        else:
            if not (([dtype for name, dtype in df.dtypes if name == event_var][0])=='timestamp'):
                raise Exception('Variabel event_var i dataframe må ha datatype timestamp')
                return
            
    else:
        if not (isinstance(df, DataFrame)):
                raise Exception('Første parameter må være en dataframe.')
                return
            
        if not (isinstance(event_var, str)):
            raise Exception('Andre parameter må være en string.')
            return

        if not (isinstance(event_id, list)):
            raise Exception('Tredje parameter må være en liste med variable.')
            return

        if not ((coDate==None) | (isinstance(coDate, datetime.datetime))):
            raise Exception('Fjerde parameter må være en dato.')
            return

def getHFrames(df, pathlists, keepvar=False):
    '''
    This functions takes a hierarchical dataframe and returns the dataframes of interest from the hierarchical structure. The frames returned
    are indicated in a supplied list to the function. This function relies on the function unpack_parquet for the unpacking of data.
    
    :param df: The hierarchical dataframe which contains elements of interest to be unpacked and returned
    :type df: dataframe
    :param pathlists: List or lists of path to dataframe to be unpacked. Position in list starts at 0 with name of variable in root level and each subsequent element 
    contains the variable name of the next level
    :param keepvar: List of root id variables in hierarchical structure to transfer to the unpacked dataframes. Default is False which leads to no variable transfer
    :type keepvar: list or boolean
    
    Returns: dictionary of dataframes or dataframe
    '''
    dicts = {} 
    keep_keys = []
    return_dict = {}
    df_hier = df
    if (isinstance(pathlists[0], list)):
        for elementlists in pathlists:
            for element in range(0, len(elementlists)):
                if element==0:
                    name = elementlists[element]
                    nokkel=keepvar
                else:
                    if (nokkel) and (element>1):
                        nokkel.append(str(elementlists[element-1])+'_id')
                    name = name + '_' + elementlists[element]                
                if name in dicts.keys():
                    df_hier = dicts[name]
                else:
                    tmpdict = unpack_parquet(df_hier, rootvar=nokkel, rootdf=False, levels=1)
                    df_hier = tmpdict[elementlists[element]]
                    dicts[name] = df_hier 
                if element == (len(elementlists)-1):
                    keep_keys.append(name)
                    df_hier = df
                    
    else:
        for element in range(0, len(pathlists)):
                if element==0:
                    name = pathlists[element]
                    nokkel=keepvar
                else:
                    if (nokkel) and (element>1):
                        nokkel.append(str(pathlists[element-1])+'_id')
                    name = name + '_' + pathlists[element]
                
                if name in dicts.keys():
                    df_hier = dicts[name]
                else:
                    tmpdict = unpack_parquet(df_hier, rootvar=keepvar, rootdf=False, levels=1)
                    df_hier = tmpdict[pathlists[element]]
                    dicts[name] = df_hier 
                if element == (len(pathlists)-1):
                    keep_keys.append(name)
                    
    for k in dicts.keys():
        if k in keep_keys:
            return_dict[k] = dicts[k]            
    if (isinstance(pathlists[0], list)):
        return return_dict
    else:
        return return_dict[keep_keys[0]]

def orderedgroup(df, groupby, orderby,null_last=True, asc = False, rankedvar='ranked'):
    """
    This function takes a dataframe and adds a variable indicating the ordered rank of variables values inside another variables grouped values
    
    :param df: Dataframe containing variable to group by and a different variable to order by inside the grouped values.
    :type df: dataframe
    :param groupby: Dataframe column to groupby
    :type groupby: string
    :param orderby: Dataframe to orderby
    :type orderby: string
    :param null_last: Variable indicating whether null values would be sorted last or first. Default value is True, sorting null values last
    :type null_last: boolean
    :param asc: Variable indicating whether ordering should be done ascending or descending. Default value is False, sorting values in a descending order
    :type asc: boolean
    :param rankedvar: Name of new column to contain the ranked value. Default name is ranked
    :type rankedvar: string
    
    Returns: dataframe
    Dataframe: Dataframe containing a new column with ranked values
    """
    
    if asc==True:
        if null_last==True:
            w = Window.partitionBy(groupby).orderBy(F.col(orderby).asc_nulls_last())
        else:
            w = Window.partitionBy(groupby).orderBy(F.col(orderby).asc_nulls_first())
    else:
        if null_last==True:
            w = Window.partitionBy(groupby).orderBy(F.col(orderby).desc_nulls_last())
        else:
            w = Window.partitionBy(groupby).orderBy(F.col(orderby).desc_nulls_first())

    df_result = df.withColumn(rankedvar,(F.row_number()).over(w).cast(StringType()))
    return df_result
            
def traverse_hierarchy(keylist, travdf, parqdf, idstreng, idname, hierarchylevels):
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
    :param idname: name of dataframe when unpacked
    :type idname: string
    :param hierarchylevels: controls the depth at which the function will look for arrays 
    :type hierarchylevels: numeric value
    :param spark_session: defines the Spark session to be used by the function. Default is None, which
                        means the function tries to catch the current session.
     :type spark_session: string
     
     Returns: 
     None: Returns nothing, but updates dictionary created earlier
    
    '''
    
    global ds_dict
    id = idname + str("_id")
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
            if socol in idstreng:
                socol_id = str(socol)+'-child'
            else: 
                socol_id = socol
            idstreng.append(socol_id)
            keylist.append(id)
            traverse_hierarchy(keylist, socol, df, idstreng, socol_id, hierarchylevels)
            keylist.remove(id)
            df = df.drop(socol)
            idstreng.remove(socol_id)
        
    dictName = '_'.join(map(str, idstreng)) 
    ds_dict[dictName]= df.cache()
        
def unpack_parquet(parqdf, rootdf=False, rootvar=True, levels=-1, spark_session=None):
    
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
    :param spark_session: defines the Spark session to be used by the function. Default is None, which
                        means the function tries to catch the current session.
    :type spark_session: string
    
     Returns: a dictionary
     Dictionary: A dictionary of unpacked dataframes.
    '''
    
    if spark_session is None:
        spark = SparkSession.builder.getOrCreate()            
    else:
        spark = spark_session
    
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
            socol_id = socol
            traverse_hierarchy(keylist, socol, parqdf, idstreng, socol_id, hierarchylevels)
            
    return ds_dict.copy()
        
