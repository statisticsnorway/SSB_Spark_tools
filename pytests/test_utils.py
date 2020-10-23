import pytest
#from ssb_sparktools.processing import processing as stproc
#from ssb_sparktools.editing import editing as stedit

import os
import sys
sys.path.append(os.path.abspath("/home/jovyan/SSB_Spark_tools/ssb_sparktools/processing/"))
sys.path.append(os.path.abspath("/home/jovyan/SSB_Spark_tools/ssb_sparktools/editing/"))
from processing import *
from editing import *

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkContext


spark = SparkSession.builder.getOrCreate() 

test_hierarkidata = spark.read.parquet("pytests/testdata/unpack_test_data.parquet").drop('identifikator',
                                                                         'gjelderPeriode',
                                                                         'hendelsetidspunkt',
                                                                         'sekvensnummer',
                                                                         'hendelsetype',
                                                                         'personidentifikator',
                                                                         'inntektsaar',
                                                                         'skjermet',
                                                                         'registreringstidspunkt')

#### unpack_parquet ####
test_dict={}

#test_dict = stproc.unpack_parquet(test_data, levels=1, spark_session=spark)
test_dict = unpack_parquet(test_hierarkidata, levels=1, spark_session=spark)

def test_unpack_parquet_return_dict():
    assert len(test_dict)!=0
    
def test_unpack_parquet_column_rettention():
    assert len(test_dict.keys())==len(test_hierarkidata.columns)
    
####  spark_missing_correction_bool  #####
testdata_schema = StructType([StructField('identifikator',StringType(),False),StructField('numbvar',LongType(),True),StructField('boolvar',BooleanType(),True)])
testdata_raw = [('id1', 1, True),('id2', 2, False), ('id3', None, False), ('id4', 4, None)]
testdata = spark.createDataFrame(testdata_raw, testdata_schema)

#test_booldata_korrigert, missing_boolcount = stedit.spark_missing_correction_bool(testdata, spark_session=spark)
test_booldata_korrigert, missing_boolcount = spark_missing_correction_bool(testdata, spark_session=spark)

def test_bool_correction():
    assert missing_boolcount.collect()[0][2]==1
