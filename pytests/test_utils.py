import pytest
#from ssb_sparktools.processing import processing as stproc
#from ssb_sparktools.editing import editing as stedit

import os
import sys
sys.path.append(os.path.abspath(os.getcwd()+"/ssb_sparktools/processing/"))
sys.path.append(os.path.abspath(os.getcwd()+"/ssb_sparktools/editing/"))
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
    
####  SPARK TOOLS EDITING  #####

# Testdata
testdata_schema = StructType([StructField('identifikator',StringType(),False),StructField('numbvar',LongType(),True),StructField('boolvar',BooleanType(),True)\
                             ,StructField('varenummer',StringType(),True)])
testdata_raw = [('id1', 1, True, '#001'),('id2', 2, False, '#002'), ('id3', None, False, '#003'), ('id4', 4, None, '#004'), ('id5', 4, True, '#005')]
testdata = spark.createDataFrame(testdata_raw, testdata_schema)

testdata_kodeliste_schema = StructType([StructField('varenummer',StringType(),True), StructField('frukt',StringType(),True)])
testdata_kodeliste_raw = [('#001', 'Eple'),('#002', 'Banan'), ('#003', 'Kiwi'), ('#004', 'Appelsin'), ('#006', 'Druer')]
testdata_kodeliste = spark.createDataFrame(testdata_kodeliste_raw, testdata_kodeliste_schema)

# listcode_lookup
lookup_result = listcode_lookup(testdata, 'varenummer', testdata_kodeliste, ['varenummer', 'frukt'], spark_session=spark)

def test_listcode_lookup():
    assert [row['frukt'] for row in lookup_result.select('frukt').collect()][:5]==['Eple', 'Banan', 'Kiwi', 'Appelsin', None]

# missing_correction_bool
test_corrected_bool, missing_boolcount = missing_correction_bool(testdata, df_name='testdf', spark_session=spark)

def test_bool_nocorrection():
    assert missing_boolcount.iat[0,3]==1

def test_bool_distribution():
    assert [row['boolvar'] for row in test_corrected_bool.select('boolvar').collect()][:4] == [True, False, False, False]

# missing_correction_number
test_corrected_number, missing_numbcount = missing_correction_number(testdata, df_name='testdf', spark_session=spark)    

def test_numb_nocorrection():
    assert missing_numbcount.iat[0,3]==1

def test_numb_distribution():
    assert [row['numbvar'] for row in test_corrected_number.select('numbvar').collect()][:4] == [1, 2, 0, 4]

# spark_missing_correction_bool
#test_booldata_korrigert, missing_boolcount = stedit.spark_missing_correction_bool(testdata, spark_session=spark)
test_sparkbooldata_korrigert, spark_missing_boolcount = spark_missing_correction_bool(testdata, spark_session=spark)

def test_sparkbool_nocorrection():
    assert spark_missing_boolcount.collect()[0][2]==1

def test_sparkbool_distribution():
    assert [row['boolvar'] for row in test_sparkbooldata_korrigert.select('boolvar').collect()][:4] == [True, False, False, False]

# spark_missing_correction_number
#test_numbdata_korrigert, missing_numbcount = stedit.spark_missing_correction_bool(testdata, spark_session=spark)
test_numbdata_korrigert, missing_numbcount = spark_missing_correction_number(testdata, spark_session=spark)

def test_numb_nocorrection():
    assert missing_numbcount.collect()[0][2]==1

def test_numb_distribution():
    assert [row['numbvar'] for row in test_numbdata_korrigert.select('numbvar').collect()][:4] == [1, 2, 0, 4]
