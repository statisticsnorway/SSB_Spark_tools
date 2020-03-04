import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark import Row


def traverse_hieark(keylist, travdf, parqdf, idstreng):
    global ds_dict
    id = travdf + "_id"
    if (len(keylist) > 0):
        df = parqdf.select(*keylist, travdf)
        if str(df.schema[travdf].dataType)[:20] == "ArrayType(StructType":
            df = df.withColumn('sonavn', F.explode(travdf))
        elif str(df.schema[travdf].dataType)[:15] == "StructType(List":
            df = df.withColumnRenamed(travdf, 'sonavn')

        df = df.withColumn(id, F.monotonically_increasing_id() + 1000000)
        sosublist = df.select('sonavn.*').columns

        newlist = []
        renvar = []

        for a in sosublist:
            if a in keylist:
                b = '{}_tmpid'.format(a)
                df = df.withColumnRenamed(a, b)
                renvar.append(b)
                newlist.append(b)

        for k in keylist:
            if "{}_tmpid".format(k) not in renvar:
                newlist.append(k)

        df = df.select(*newlist, id, 'sonavn.*')

        for b in renvar:
            a = b[:-6]
            df = df \
                .withColumnRenamed(a, "{}_{}".format(a, travdf)) \
                .withColumnRenamed(b, a)

    else:
        df = parqdf.select(travdf)
        if str(df.schema[travdf].dataType)[:20] == "ArrayType(StructType":
            df = df.withColumn('sonavn', F.explode(travdf))
        elif str(df.schema[travdf].dataType)[:15] == "StructType(List":
            df = df.withColumnRenamed(travdf, 'sonavn')
        df = df.withColumn(id, F.monotonically_increasing_id() + 1000000) \
            .select(id, 'sonavn.*')

    dictName = '_'.join(map(str, idstreng))

    ds_dict[dictName] = df.cache()
    cols = [i.name for i in df.schema.fields if
            ("ArrayType(StructType" == str(i.dataType)[:20]) | ("StructType(List" == str(i.dataType)[:15])]

    for socol in cols:
        idstreng.append(socol)
        keylist.append(id)
        traverse_hieark(keylist, socol, df, idstreng)
        keylist.remove(id)
        df = df.drop(socol)
        idstreng.remove(socol)


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

    if (rootdf != False):
        if rootdf == True:
            for i in parqdf.schema.fields:
                if (str(i.dataType)[:20] != "ArrayType(StructType") & (str(i.dataType)[:15] != "StructType(List"):
                    keylist.append(i.name)
        else:
            keylist = rootdf

        if len(keylist) > 0:
            ds_dict['rootdf'] = parqdf.select(*keylist)

    keylist = []
    if (rootvar != False):
        if (rootvar == True):
            for i in parqdf.schema.fields:
                if (str(i.dataType)[:20] != "ArrayType(StructType") & (str(i.dataType)[:15] != "StructType(List"):
                    keylist.append(i.name)
        else:
            keylist = rootvar
    else:
        keylist = []

    list_col = [i.name for i in parqdf.schema.fields if
                ("ArrayType(StructType" == str(i.dataType)[:20]) | ("StructType(List" == str(i.dataType)[:15])]

    for socol in list_col:
        idstreng = [socol]
        traverse_hieark(keylist, socol, parqdf, idstreng)
    return ds_dict.copy()